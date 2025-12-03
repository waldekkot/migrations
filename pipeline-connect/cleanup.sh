#!/bin/bash
# Cleanup script to empty old_versions, output directories, and Jupyter checkpoints

set -e

OLD_VERSIONS_DIR="source_code/old_versions"
OUTPUT_DIR="source_code/output"
SOURCE_CODE_DIR="source_code"

echo "Cleanup Script"
echo "================================="
echo "This will delete all files from:"
echo "  - ${OLD_VERSIONS_DIR}/"
echo "  - ${OUTPUT_DIR}/"
echo "  - .ipynb_checkpoints/ (Jupyter)"
echo "================================="
echo ""

# Function to get directory size and file count
get_dir_info() {
    local dir=$1
    if [ -d "$dir" ]; then
        local file_count=$(find "$dir" -type f | wc -l | tr -d ' ')
        local dir_size=$(du -sh "$dir" 2>/dev/null | awk '{print $1}')
        echo "$file_count files ($dir_size)"
    else
        echo "Directory not found"
    fi
}

# Function to count checkpoint directories
count_checkpoints() {
    local dir=$1
    if [ -d "$dir" ]; then
        local checkpoint_count=$(find "$dir" -type d -name ".ipynb_checkpoints" 2>/dev/null | wc -l | tr -d ' ')
        echo "$checkpoint_count"
    else
        echo "0"
    fi
}

# Show current state
echo "📊 Current state:"
echo "  old_versions:     $(get_dir_info "$OLD_VERSIONS_DIR")"
echo "  output:           $(get_dir_info "$OUTPUT_DIR")"
echo "  .ipynb_checkpoints: $(count_checkpoints "$SOURCE_CODE_DIR") directories"
echo ""

# Confirmation prompt
read -p "⚠️  Are you sure you want to delete all files? (y/N): " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ Cleanup cancelled"
    exit 0
fi

echo ""
echo "🧹 Cleaning up..."
echo "================================="

# Clean old_versions directory
if [ -d "$OLD_VERSIONS_DIR" ]; then
    file_count=$(find "$OLD_VERSIONS_DIR" -type f | wc -l | tr -d ' ')
    if [ "$file_count" -gt 0 ]; then
        rm -f "$OLD_VERSIONS_DIR"/*
        echo "✅ Removed $file_count file(s) from old_versions/"
    else
        echo "ℹ️  old_versions/ was already empty"
    fi
else
    echo "⚠️  old_versions/ directory not found (skipping)"
fi

# Clean output directory
if [ -d "$OUTPUT_DIR" ]; then
    file_count=$(find "$OUTPUT_DIR" -type f | wc -l | tr -d ' ')
    if [ "$file_count" -gt 0 ]; then
        rm -f "$OUTPUT_DIR"/*
        echo "✅ Removed $file_count file(s) from output/"
    else
        echo "ℹ️  output/ was already empty"
    fi
else
    echo "⚠️  output/ directory not found (skipping)"
fi

# Clean Jupyter checkpoint directories
if [ -d "$SOURCE_CODE_DIR" ]; then
    checkpoint_dirs=$(find "$SOURCE_CODE_DIR" -type d -name ".ipynb_checkpoints" 2>/dev/null)
    checkpoint_count=$(echo "$checkpoint_dirs" | grep -c "." 2>/dev/null || echo "0")
    
    if [ "$checkpoint_count" -gt 0 ]; then
        echo "$checkpoint_dirs" | while read -r checkpoint_dir; do
            if [ -n "$checkpoint_dir" ] && [ -d "$checkpoint_dir" ]; then
                rm -rf "$checkpoint_dir"
            fi
        done
        echo "✅ Removed $checkpoint_count .ipynb_checkpoints/ director(y/ies)"
    else
        echo "ℹ️  No .ipynb_checkpoints/ directories found"
    fi
else
    echo "⚠️  source_code/ directory not found (skipping)"
fi

echo ""
echo "================================="
echo "✅ Cleanup complete!"
echo ""
echo "📊 Final state:"
echo "  old_versions:     $(get_dir_info "$OLD_VERSIONS_DIR")"
echo "  output:           $(get_dir_info "$OUTPUT_DIR")"
echo "  .ipynb_checkpoints: $(count_checkpoints "$SOURCE_CODE_DIR") directories"
echo ""
echo "Directories are now clean and ready for fresh runs."

