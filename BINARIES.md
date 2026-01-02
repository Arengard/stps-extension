# Pre-built Binaries - How It Works

This document explains how the automated binary building and committing system works.

## Overview

This repository includes a GitHub Actions workflow that automatically builds pre-compiled extension binaries for multiple platforms whenever code changes are pushed to the main branches.

## When Binaries Are Built

The workflow (`.github/workflows/build-and-commit-binaries.yml`) is triggered when:

- Changes are pushed to branches: `master`, `main`, `develop`, or `dev`
- AND the changes affect any of these paths:
  - `src/**` (source code files)
  - `CMakeLists.txt`
  - `extension_config.cmake`
  - `Makefile`

## Build Process

1. **Parallel Builds**: The workflow builds binaries for 4 platforms in parallel:
   - Linux (x86_64)
   - macOS Apple Silicon (ARM64)
   - macOS Intel (x86_64)
   - Windows (x86_64)

2. **Artifact Collection**: Each build uploads its binary as an artifact

3. **Commit Phase**: After all builds complete successfully:
   - All binaries are downloaded
   - They are organized in the `binaries/` directory structure
   - A single commit is made with all updated binaries
   - The commit message includes `[skip ci]` to prevent infinite loops

## Directory Structure

```
binaries/
├── README.md                             # User-facing documentation
├── stps_linux_amd64.duckdb_extension     # Linux (x86_64) binary
├── stps_osx_arm64.duckdb_extension       # macOS Apple Silicon (ARM64) binary
├── stps_osx_amd64.duckdb_extension       # macOS Intel (x86_64) binary
└── stps_windows_amd64.duckdb_extension   # Windows (x86_64) binary
```

## For Users

### After Cloning or Pulling

When you clone the repository or run `git pull`, you automatically get the latest pre-built binaries. No compilation needed!

```bash
# Clone the repo
git clone https://github.com/Arengard/stps-extension.git
cd stps-extension

# Check which binary to use
./check-binary.sh

# Or use directly
duckdb -c "INSTALL './binaries/linux_amd64/stps.duckdb_extension'; LOAD stps;"
```

### Updating Binaries

Simply pull the latest changes:

```bash
git pull
```

If source code was updated, the workflow will have automatically built and committed new binaries.

## For Contributors

### Making Code Changes

When you make changes to the source code and push to a main branch:

1. Your code changes are pushed
2. CI workflow builds new binaries (takes ~10-30 minutes)
3. Binaries are automatically committed back to the repository
4. Future pulls will include the updated binaries

### Important Notes

- The binaries are committed with `[skip ci]` to avoid triggering another build
- Only changes to source files trigger builds (not documentation, etc.)
- Binaries are built in **release mode** for optimal performance
- Each platform binary includes build metadata in its README

### Testing the Workflow

To test if the workflow will trigger:

1. Make a change to a file in `src/`
2. Push to `develop` or `dev` branch
3. Check the Actions tab on GitHub
4. Wait for the "Build and Commit Binaries" workflow to complete
5. Pull the branch to get the new binaries

## Limitations

- **Storage**: Binary files increase repository size. This is a trade-off for user convenience.
- **Build Time**: It takes time for binaries to be built and committed after code changes.
- **Platform Coverage**: Only the 4 main platforms are supported. Other platforms need manual building.

## Alternative: Building from Source

If you need:
- A different platform
- Debug binaries
- Custom build flags

See the main [README.md](README.md) for build instructions.

## Troubleshooting

### Binaries Not Updated After Pull

1. Check if your changes triggered the workflow:
   - Go to GitHub Actions tab
   - Look for "Build and Commit Binaries" workflow
   - Check if it ran and succeeded

2. If workflow didn't trigger:
   - Changes might not have affected trigger paths
   - Check `.github/workflows/build-and-commit-binaries.yml` for trigger conditions

3. If workflow failed:
   - Check the workflow logs on GitHub
   - Build might have failed on a specific platform
   - Open an issue with the error details

### Binary Doesn't Work

1. Verify you're using the correct platform binary
2. Run `./check-binary.sh` to confirm platform detection
3. Try building from source to verify the code compiles
4. Check DuckDB version compatibility

## Repository Maintenance

The `binaries/` directory is tracked in git (not in `.gitignore`). This is intentional to provide pre-built binaries directly in the repository.

If repository size becomes an issue, consider:
- Using Git LFS for binary files
- Moving to GitHub Releases instead of committing binaries
- Reducing the number of supported platforms
