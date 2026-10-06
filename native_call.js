// Codex code-mode entry. Execute inside functions.exec with the real tools object.
// It uses the current, already-running conversation's tool context, never a new model.
return (async () => {
  const hub = typeof hubPath === "string" && hubPath ? hubPath : "./hub.py";
  const quote = value => "'" + String(value).replace(/'/g, "'\\''") + "'";
  const call = async (op, args) => {
    let response = await tools.exec_command({
      cmd: "PYTHONDONTWRITEBYTECODE=1 python3 " + quote(hub) + " call " + quote(op) + " --json " + quote(JSON.stringify(args)),
      max_output_tokens: 6500
    });
    let output = response.output || "";
    const deadline = Date.now() + 90000;
    while (response.session_id) {
      if (Date.now() >= deadline || typeof tools.write_stdin !== "function") {
        throw new Error("中枢命令仍在运行，账本结果待核对，禁止重领或重发");
      }
      response = await tools.write_stdin({ session_id: response.session_id,
        chars: "", yield_time_ms: 1000, max_output_tokens: 6500 });
      output += response.output || "";
      if (output.length > 200000) throw new Error("中枢输出超限，账本结果待核对");
    }
    let value;
    try { value = JSON.parse(output); }
    catch { throw new Error("中枢返回不完整或截断，账本结果待核对"); }
    if (response.exit_code !== 0 || !value.ok) throw new Error(value.error || "中枢命令未正常结束，账本结果待核对");
    return value.result;
  };
  const decode = result => {
    if (result.isError) throw new Error("原生工具未确认成功：" + String(result.content?.find(x => x.type === "text")?.text || "未提供原因").slice(0, 600));
    const block = result.content?.find(x => x.type === "text");
    if (!block) throw new Error("原生工具未返回状态");
    return JSON.parse(block.text);
  };
  // Read only bounded native user-message records. A newer UUID or idle status
  // alone never establishes which turn belongs to this delivery.
  const findSentTurn = async (threadId, message, pages = 1) => {
    if (typeof tools.mcp__codex_app__read_thread !== "function") return null;
    let cursor;
    for (let page = 0; page < pages; page++) {
      const read = decode(await tools.mcp__codex_app__read_thread({ threadId, turnLimit: 5,
        includeOutputs: false, maxOutputCharsPerItem: Math.min(16000, message.length + 100),
        ...(cursor ? { cursor } : {}) }));
      if (read.thread?.id !== threadId || !Array.isArray(read.turns)) throw new Error("原生历史目标未知");
      const matches = read.turns.filter(turn => typeof turn.id === "string" && turn.items?.some(item =>
        item.type === "userMessage" && Array.isArray(item.content) &&
        item.content.filter(part => part.type === "text").map(part => part.text).join("\n") === message));
      if (matches.length > 1) throw new Error("原消息对应多个轮次，须人工核对");
      if (matches.length === 1) return matches[0].id;
      cursor = read.page?.nextCursor;
      if (!cursor || !read.page?.hasMore) break;
    }
    return null;
  };
  if (operation === "delivery_link_native") {
    const keys = ["delivery_id", "expected_thread_id"];
    if (!payload || Object.keys(payload).some(key => !keys.includes(key)) ||
        keys.some(key => typeof payload[key] !== "string" || !payload[key])) throw new Error("核对只接收投递和目标编号");
    const ticket = await call("delivery_link_native_prepare", payload);
    const actual = await findSentTurn(ticket.thread_id, ticket.message, 3);
    if (!actual) return { result: { linked: false, error: "有界原生历史未找到原消息，保留投递，禁止重发" } };
    return { result: await call("delivery_link_native_commit", { ...payload, token: ticket.token,
      proof: { thread_id: ticket.thread_id, turn_id: actual, user_message: ticket.message } }) };
  }
  if (operation.startsWith("delivery_link_native_")) throw new Error("内部关联步骤须经原生入口");
  let operationResult;
  let reconcileTarget;
  if (operation === "delivery_reconcile_native") {
    // Public parameters are identifiers only. Evidence must come from native tools here.
    const keys = ["delivery_id", "expected_thread_id", "expected_turn_id"];
    if (!payload || Object.keys(payload).some(key => !keys.includes(key)) ||
        keys.some(key => typeof payload[key] !== "string" || !payload[key])) {
      throw new Error("核对仅接收delivery_id、expected_thread_id、expected_turn_id");
    }
    if (typeof tools.mcp__codex_app__wait_threads !== "function" ||
        typeof tools.mcp__codex_app__read_thread !== "function") {
      throw new Error("缺少原生核对工具，保留原投递");
    }
    const ticket = await call("delivery_reconcile_native_prepare", payload);
    const turn = value => value && ({ id: value.id, status: value.status,
      error: value.error ?? null, startedAt: value.startedAt, completedAt: value.completedAt });
    const snapshot = async () => {
      const result = decode(await tools.mcp__codex_app__wait_threads({
        targets: [{ threadId: ticket.thread_id }], timeoutMs: 0
      }));
      if (result.errors?.length) throw new Error("原生状态含错误，保留原投递");
      const found = result.polls?.find(p => p.thread?.id === ticket.thread_id);
      if (!found || found.thread.status?.type !== "idle" || found.latestTurn?.status !== "completed") {
        throw new Error("接收方活动或状态未知，保留原投递");
      }
      return { thread_id: found.thread.id, status: found.thread.status.type, latest: turn(found.latestTurn) };
    };
    let commitAttempted = false;
    try {
      const before = await snapshot();
      const history = { thread_id: ticket.thread_id, status: "idle", turns: [] };
      let cursor;
      // Bounded history lookup: missing old turn is a refusal, not permission to infer.
      for (let page = 0; page < 3; page++) {
        const read = decode(await tools.mcp__codex_app__read_thread({ threadId: ticket.thread_id,
          turnLimit: 5, includeOutputs: false,
          maxOutputCharsPerItem: 100,
          ...(cursor ? { cursor } : {}) }));
        if (read.thread?.id !== ticket.thread_id || read.thread.status?.type !== "idle" ||
            read.page?.order !== "newest_first" || !Array.isArray(read.turns)) {
          throw new Error("原生历史目标或状态未知，保留原投递");
        }
        for (const item of read.turns) {
          history.turns.push(turn(item));
          if (item.id === ticket.turn_id) break;
        }
        if (history.turns.some(item => item.id === ticket.turn_id)) break;
        cursor = read.page?.nextCursor;
        if (!cursor || !read.page?.hasMore) break;
      }
      const after = await snapshot();
      commitAttempted = true;
      operationResult = await call("delivery_reconcile_native_commit", {
        ...payload, token: ticket.token, proof: { before, history, after }
      });
      reconcileTarget = ticket.thread_id;
    } catch (error) {
      return { result: { delivery_id: payload.delivery_id, reconciled: false,
        commitAttempted, error: String(error) } };
    }
  } else {
    if (operation.startsWith("delivery_reconcile_native_")) {
      throw new Error("内部核对步骤只由delivery_reconcile_native调用");
    }
    operationResult = operation === "drain" ? {} : await call(operation, payload);
  }
  if (!new Set(["begin", "end", "request", "request_update", "version_create", "version_review", "delivery_retry", "budget_report", "budget_decide", "drain", "delivery_reconcile_native"]).has(operation)) {
    return { result: operationResult };
  }
  const batch = await call("delivery_candidates", {});
  const outcomes = [];
  if (typeof tools.mcp__codex_app__send_message_to_thread !== "function" ||
      typeof tools.mcp__codex_app__wait_threads !== "function") {
    return { result: operationResult, delivery: { pending: batch.deliveries.length,
      error: "当前入口无 Codex 原生工具，保留队列，不允许隐藏后备执行" } };
  }
  for (const item of batch.deliveries) {
    if (item.thread_id === batch.calling_thread_id) continue;
    if (reconcileTarget && item.thread_id !== reconcileTarget) continue;
    let claimed;
    let claimAttempted = false;
    try {
      const status = decode(await tools.mcp__codex_app__wait_threads({
        targets: [{ threadId: item.thread_id }], timeoutMs: 0
      }));
      const snapshot = status.polls?.find(p => p.thread?.id === item.thread_id);
      if (!snapshot || snapshot.thread.status.type === "active" ||
          (reconcileTarget && snapshot.thread.status.type !== "idle")) {
        outcomes.push({ delivery_id: item.delivery_id, state: "waiting_for_current_turn" });
        continue;
      }
      claimAttempted = true;
      claimed = await call("delivery_claim_native", { delivery_id: item.delivery_id });
      if (claimed.skipped) {
        outcomes.push({ delivery_id: item.delivery_id, state: claimed.state });
        claimed = undefined;
        continue;
      }
      if (claimed.thread_id !== item.thread_id) throw new Error("投递目标不一致");
      const sent = decode(await tools.mcp__codex_app__send_message_to_thread({
        threadId: claimed.thread_id, prompt: claimed.message
      }));
      if (sent.threadId !== claimed.thread_id) throw new Error("原生接收目标未确认");
      const after = decode(await tools.mcp__codex_app__wait_threads({
        targets: [{ threadId: claimed.thread_id }], timeoutMs: 0
      }));
      const actual = after.polls?.find(p => p.thread?.id === claimed.thread_id);
      let actualTurn = sent.turnId || sent.turn?.id;
      if (!actualTurn) {
        try { actualTurn = await findSentTurn(claimed.thread_id, claimed.message); }
        catch { /* Confirmed receipt remains recorded; bounded recovery is explicit. */ }
      }
      await call("delivery_receipt_native", { delivery_id: item.delivery_id, confirmed: true,
        ...(actualTurn ? { turn_id: actualTurn } : {}) });
      outcomes.push({ delivery_id: item.delivery_id, thread_id: claimed.thread_id,
        state: actual?.thread?.status?.type || "delivered", turn_id: actualTurn,
        ...(actualTurn ? {} : { needs_turn_link: true }) });
    } catch (error) {
      if (claimed) {
        try { await call("delivery_receipt_native", { delivery_id: item.delivery_id, confirmed: false }); }
        catch { /* Keep uncertain/sending for reconciliation; never blind retry. */ }
      }
      outcomes.push({ delivery_id: item.delivery_id, state: claimAttempted ? "uncertain" : "pending", error: String(error) });
    }
  }
  return { result: operationResult, delivery: outcomes };
})();
