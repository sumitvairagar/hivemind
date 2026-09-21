import { existsSync, readFileSync, unlinkSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { homedir } from "node:os";
import { ensureDir, log } from "./util.js";
import { ensureMcpServerInstalled, buildMcpServerEntry } from "./install-mcp-shared.js";

// Kiro CLI integration.
//
// Kiro reads MCP server configuration from:
//   ~/.kiro/settings/mcp.json
//
// Format is identical to the Claude Code / Cursor pattern:
//   { "mcpServers": { "<name>": { "command": "...", "args": [...] } } }
//
// The installer registers the shared hivemind MCP server (already installed
// at ~/.hivemind/mcp/server.js) so Kiro sessions gain hivemind_search /
// hivemind_read / hivemind_index tools with zero manual setup.

const HOME = homedir();
const KIRO_SETTINGS_DIR = join(HOME, ".kiro", "settings");
const CONFIG_PATH = join(KIRO_SETTINGS_DIR, "mcp.json");
const SERVER_KEY = "hivemind";

type McpConfig = Record<string, unknown>;

function readConfig(): McpConfig {
  if (!existsSync(CONFIG_PATH)) return {};
  const txt = readFileSync(CONFIG_PATH, "utf-8").trim();
  if (!txt) return {};
  let parsed: unknown;
  try {
    parsed = JSON.parse(txt);
  } catch {
    // Malformed config — never clobber the user's file.
    throw new Error(
      `mcp.json at ${CONFIG_PATH} is not valid JSON. Fix or remove it, then rerun.`,
    );
  }
  return parsed && typeof parsed === "object" ? (parsed as McpConfig) : {};
}

function writeConfig(cfg: McpConfig): void {
  ensureDir(KIRO_SETTINGS_DIR);
  writeFileSync(CONFIG_PATH, JSON.stringify(cfg, null, 2) + "\n");
}

export function installKiro(): void {
  // 1. Shared stdio MCP server binary at ~/.hivemind/mcp/server.js.
  ensureMcpServerInstalled();

  // 2. Register it in ~/.kiro/settings/mcp.json.
  //    Non-destructive merge: preserve any servers the user already added.
  const cfg = readConfig();
  const servers =
    cfg.mcpServers && typeof cfg.mcpServers === "object"
      ? (cfg.mcpServers as Record<string, unknown>)
      : {};
  servers[SERVER_KEY] = buildMcpServerEntry();
  cfg.mcpServers = servers;
  writeConfig(cfg);
  log(`  Kiro           config updated -> ${CONFIG_PATH} (mcpServers.${SERVER_KEY})`);
}

export function uninstallKiro(): void {
  if (!existsSync(CONFIG_PATH)) return;
  let cfg: McpConfig;
  try {
    cfg = readConfig();
  } catch {
    // Malformed file — leave it alone rather than fail the uninstall.
    return;
  }
  const servers = cfg.mcpServers;
  if (!servers || typeof servers !== "object" || !(SERVER_KEY in servers)) return;

  delete (servers as Record<string, unknown>)[SERVER_KEY];
  if (Object.keys(servers as Record<string, unknown>).length === 0) delete cfg.mcpServers;

  if (Object.keys(cfg).length === 0) {
    // Config is now empty — remove the file rather than leave a {}
    unlinkSync(CONFIG_PATH);
  } else {
    writeConfig(cfg);
  }
  log(`  Kiro           hivemind entry removed from ${CONFIG_PATH}`);
}
