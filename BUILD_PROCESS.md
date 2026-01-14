# Build Process

## Automated GitHub Actions Build

**IMPORTANT:** This extension is built automatically by GitHub Actions. You do not need to build it locally.

### How It Works

Every push to any branch triggers an automated build process via GitHub Actions:

1. **Workflow File**: `.github/workflows/windows-build-on-push.yml`
2. **Build Command**: The workflow runs `make release` on Windows
3. **Artifacts**: Built extension is uploaded as a GitHub Actions artifact

### Getting the Built Extension

#### Option 1: Download from GitHub Actions (Recommended)

1. Go to the [Actions tab](https://github.com/Arengard/stps-extension/actions)
2. Click on the most recent workflow run
3. Scroll to the **Artifacts** section at the bottom
4. Download `stps-windows-amd64-latest-<branch-name>`
5. Extract the `stps.duckdb_extension` file

#### Option 2: From Tagged Releases

If a version tag is pushed, the extension is automatically published as a GitHub Release:

1. Go to [Releases](https://github.com/Arengard/stps-extension/releases)
2. Download `stps.duckdb_extension` from the latest release

### Build Artifacts

Each build produces:
- `stps.duckdb_extension` - The compiled extension
- `build-info.txt` - Build metadata (commit, date, platform)

### Artifact Retention

- **Commit-specific artifacts**: 90 days
- **Branch-latest artifacts**: 30 days
- **Release artifacts**: Permanent

### Manual Build (Not Recommended)

If you absolutely need to build locally on Windows:

```bash
# Requires Visual Studio 2022 with C++ tools
make release
```

However, this is **not recommended** because:
- Complex environment setup required
- GitHub Actions provides consistent builds
- Pre-built artifacts are readily available

### Testing the Extension

After downloading from GitHub Actions:

```sql
-- In DuckDB CLI
INSTALL './stps.duckdb_extension';
LOAD stps;

-- Test basic function
SELECT stps_is_valid_iban('DE89370400440532013000');

-- Test address lookup (requires internet)
SELECT stps_get_address('Deutsche Bank AG');
```

### Troubleshooting

**Q: I made changes, how do I get a new build?**
A: Just commit and push your changes. GitHub Actions will automatically build and upload a new artifact.

**Q: Where is the build output?**
A: Check the Actions tab for the workflow run corresponding to your commit.

**Q: Can I build locally for testing?**
A: Yes, but it's easier to let GitHub Actions handle it. If you must, see `build-windows.bat` - but note it requires full Visual Studio setup.

**Q: How long does a build take?**
A: Typically 10-20 minutes for a full build on GitHub Actions.

### CI/CD Pipeline

```
Push to GitHub
    ↓
GitHub Actions Triggered
    ↓
Setup Build Environment
    ↓
Clone Submodules (DuckDB)
    ↓
Build Extension (make release)
    ↓
Test Extension Loading
    ↓
Upload Artifacts
    ↓
(Optional) Create Release
```

## Summary

**You do not need to build locally.** Push your changes to GitHub and download the built extension from GitHub Actions artifacts.
