#!/bin/bash

# Script to set up the DuckDB extension repository for GitHub

echo "Setting up DuckDB extension repository..."

# Initialize git if not already done
if [ ! -d ".git" ]; then
    echo "Initializing git repository..."
    git init
fi

# Set up .gitignore if it doesn't exist
if [ ! -f ".gitignore" ]; then
    echo "Creating .gitignore..."
    cat > .gitignore << 'EOF'
# Build directories
build/
cmake-build-*/

# IDE files
.idea/
.vscode/
*.swp
*.swo

# OS files
.DS_Store
Thumbs.db

# vcpkg
vcpkg_installed/

# Logs
*.log
EOF
fi

# Create .gitmodules
echo "Creating .gitmodules..."
cat > .gitmodules << 'EOF'
[submodule "duckdb"]
	path = duckdb
	url = https://github.com/duckdb/duckdb.git
[submodule "extension-ci-tools"]
	path = extension-ci-tools
	url = https://github.com/duckdb/extension-ci-tools.git
EOF

# Remove existing directories if they exist but aren't submodules
if [ -d "duckdb" ] && [ ! -d "duckdb/.git" ]; then
    echo "Removing existing duckdb directory..."
    rm -rf duckdb
fi

if [ -d "extension-ci-tools" ] && [ ! -d "extension-ci-tools/.git" ]; then
    echo "Removing existing extension-ci-tools directory..."
    rm -rf extension-ci-tools
fi

# Add submodules
echo "Adding submodules..."
git submodule add https://github.com/duckdb/duckdb.git duckdb || echo "duckdb submodule already exists"
git submodule add https://github.com/duckdb/extension-ci-tools.git extension-ci-tools || echo "extension-ci-tools submodule already exists"

# Initialize and update submodules
echo "Initializing and updating submodules..."
git submodule update --init --recursive

# Add all files
echo "Adding files to git..."
git add .

# Create initial commit
echo "Creating initial commit..."
git commit -m "Initial commit: DuckDB extension with submodules and CI/CD setup" || echo "Nothing to commit"

echo ""
echo "Repository setup complete!"
echo ""
echo "To push to GitHub:"
echo "1. Create a new repository on GitHub"
echo "2. Run: git remote add origin <your-repo-url>"
echo "3. Run: git branch -M main"
echo "4. Run: git push -u origin main"
echo ""
echo "The GitHub Actions workflows are configured to build your extension automatically."
