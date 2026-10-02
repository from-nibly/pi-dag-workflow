export const DEFAULT_VERIFICATION_COMMAND_TIMEOUT_MS = 3_600_000;
// Stay within signed 32-bit timer limits; zero never means unlimited.
export const MAX_VERIFICATION_COMMAND_TIMEOUT_MS = 2_147_483_647;

export function verificationCommandTimeoutMs(value: unknown = DEFAULT_VERIFICATION_COMMAND_TIMEOUT_MS): number {
  if (typeof value !== "number" || !Number.isInteger(value) || value < 1 || value > MAX_VERIFICATION_COMMAND_TIMEOUT_MS) {
    throw Error(`INVALID_VERIFICATION_COMMAND_TIMEOUT: verificationCommandTimeoutMs must be an integer from 1 to ${MAX_VERIFICATION_COMMAND_TIMEOUT_MS} milliseconds`);
  }
  return value;
}
