import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

export const extensionDir = dirname(fileURLToPath(import.meta.url));
export const commandPromptsDir = join(extensionDir, "command-prompts");
export const stepPromptsDir = join(extensionDir, "step-prompts");
