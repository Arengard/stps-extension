# Quick Start Guide

## Setting Up the Repository for GitHub

1. **Run the setup script:**
   ```bash
   ./setup-repo.sh
   ```

2. **Create a new repository on GitHub:**
   - Go to https://github.com/new
   - Name your repository (e.g., "stps-extension")
   - Don't initialize with README (we already have one)
   - Click "Create repository"

3. **Push to GitHub:**
   ```bash
   git remote add origin https://github.com/YOUR-USERNAME/YOUR-REPO-NAME.git
   git branch -M main
   git push -u origin main
   ```

## Testing Locally

Run the test script to make sure everything works:
```bash
./test-extension.sh
```

## GitHub Actions Workflows

Your repository now includes four workflows:

### 1. **Quick Build** (`.github/workflows/quick-build.yml`)
- Runs on every push to main/develop branches
- Fast Linux-only build for development feedback
- Includes comprehensive debugging output to troubleshoot build issues
- Uses caching for faster builds

### 2. **Full Build** (`.github/workflows/build.yml`) 
- Runs on pushes and pull requests
- Builds on both Linux and macOS
- Includes testing and comprehensive file location debugging
- Automatically finds extension files regardless of build location

### 3. **Release Build** (`.github/workflows/release.yml`)
- Runs on git tags (e.g., `v1.0.0`)
- Creates release artifacts for multiple platforms
- Automatically uploads to GitHub releases

### 4. **Debug Build** (`.github/workflows/debug-build.yml`)
- Manual workflow for deep debugging build issues
- Shows complete directory trees and file locations
- Can be triggered manually from GitHub Actions tab

## Common Issues and Solutions

### Build Issues Fixed
The workflows now automatically:
- ✅ Handle git ownership issues in CI containers
- ✅ Find extension files in any build location
- ✅ Provide detailed debugging output when builds fail
- ✅ Test extension loading with proper file paths

### Extension File Not Found
If you see "extension file not found" errors, the new workflows will:
- Show exactly where files are being built
- Search for extension files in all possible locations  
- Display the complete build directory structure

### Submodule Problems
If submodules don't initialize properly:
```bash
git submodule update --init --recursive --force
```

### Build Issues
Clean build:
```bash
make clean
make debug
```

### Git Ownership (in CI)
The workflows automatically handle git ownership issues that occur in GitHub Actions containers.

## Next Steps

1. Push your code to GitHub
2. Watch the workflows run automatically
3. Create a git tag to trigger a release: `git tag v1.0.0 && git push origin v1.0.0`
4. Customize the extension functions in the `src/` directory
