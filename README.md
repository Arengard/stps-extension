# STPS Extension

The **STPS Extension** is a developer productivity extension that integrates STPS tooling into your IDE, providing code analysis, navigation, and workflow automation tailored for STPS-based projects.

## Features

- **STPS project discovery** – Automatically detects STPS projects and configures the extension accordingly.
- **Code navigation** – Go to definitions, find references, and explore STPS symbols directly from your editor.
- **Diagnostics & linting** – Surface STPS-related errors and warnings inline as you edit.
- **Commands & tasks** – Run common STPS commands (build, test, deploy, etc.) from the command palette or task runner.
- **Configuration support** – Respect project-level and user-level STPS configuration files.

> Note: The exact feature set may vary depending on the version of the extension and the host IDE.

## Installation

### From the extension marketplace

1. Open your IDE’s **Extensions** or **Plugins** view.
2. Search for **“STPS Extension”**.
3. Click **Install**.
4. Reload or restart the IDE if prompted.

### From a packaged binary

If you have a prebuilt extension package (e.g., `.vsix` or similar):

1. Open the **Extensions** or **Plugins** view.
2. Choose **Install from VSIX / local file** (exact wording depends on your IDE).
3. Select the downloaded STPS Extension binary.
4. Confirm and reload the IDE.

## Usage

1. Open an existing **STPS project** in your IDE.
2. Ensure any required STPS CLI or SDK is installed and on your `PATH`.
3. Wait for the extension to initialize; you should see STPS-specific status indicators or messages in the output/log panel.
4. Use the following common actions:
   - **Command palette** → type `STPS:` to see available extension commands.
   - **Code navigation** → right-click on a symbol and choose “Go to Definition” or “Find References”.
   - **Diagnostics** → open the “Problems” or “Diagnostics” view to inspect STPS-related issues.

### Configuration

The extension reads settings from:

- **User settings** – Global preferences for how STPS should behave in your IDE.
- **Workspace/project settings** – Project-specific overrides for tools, paths, and rules.

Refer to your IDE’s settings UI and search for **“STPS”** to see all available options.

## Development

To work on the STPS Extension itself:

1. Clone this repository:

   ```bash
   git clone https://github.com/your-org/stps-extension.git
   cd stps-extension
<!-- Trigger build: 2025-12-31 11:24:03 -->
