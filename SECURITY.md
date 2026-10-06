# Security policy

Do not include API keys, authentication files, conversation transcripts, or
private identity cards in bug reports. Redact thread IDs unless they are
synthetic reproduction values.

Use GitHub private vulnerability reporting for security-sensitive findings. For
ordinary bugs, open an issue with the smallest synthetic reproduction possible.

The hub is a same-user coordination tool. It is not an operating-system security
boundary and does not protect against malicious processes running as the same
user.

Generated documents are confined to the hub's `docs/` directory. Bootstrap
validates role slugs and all onboarding fields before committing, and generated
paths reject symbolic links. These protections prevent accidental configuration
escapes; they do not claim protection from a malicious same-user filesystem race.

Worker shutdown uses a local control socket owned by the locked worker. It does
not signal process IDs read from persisted state. Delivery uncertainty requires
native evidence or explicit verified retry; it never authorizes a hidden engine
or external model quota.
