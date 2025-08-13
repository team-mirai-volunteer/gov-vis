import argparse
import json
import hashlib
import os
import shutil
import sys
import tarfile
import urllib.request
import zipfile
from tqdm import tqdm

class TqdmUpTo(tqdm):
    """Provides `update_to(block_num, block_size, total_size)`."""
    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)

def get_sha256(filepath):
    """Calculate SHA256 hash of a file."""
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def verify_file(filepath, expected_sha256):
    """Verify file integrity using SHA256 hash."""
    if not os.path.exists(filepath):
        return False
    if expected_sha256 is None:
        print(f"⚠️  Warning: No SHA256 hash provided for {os.path.basename(filepath)}. Skipping verification.")
        return True
    
    print(f"   Verifying {os.path.basename(filepath)}...")
    actual_sha256 = get_sha256(filepath)
    if actual_sha256 == expected_sha256:
        print(f"   ✅ Verification successful.")
        return True
    else:
        print(f"   ❌ Verification failed. Expected {expected_sha256}, got {actual_sha256}")
        return False

def download_file(url, output_path):
    """Download a file with a progress bar."""
    filename = os.path.basename(output_path)
    with TqdmUpTo(unit='B', unit_scale=True, unit_divisor=1024, miniters=1, desc=f"   Downloading {filename}") as t:
        try:
            urllib.request.urlretrieve(url, filename=output_path, reporthook=t.update_to)
        except Exception as e:
            print(f"\n   ❌ Error downloading {url}: {e}", file=sys.stderr)
            if os.path.exists(output_path):
                os.remove(output_path)
            return False
    return True

def unpack_archive(filepath, dest_dir):
    """Unpack a zip or tar archive."""
    filename = os.path.basename(filepath)
    print(f"   Unpacking {filename} to {dest_dir}...")
    try:
        if filepath.endswith('.zip'):
            with zipfile.ZipFile(filepath, 'r') as zip_ref:
                zip_ref.extractall(dest_dir)
        elif filepath.endswith('.tar.gz') or filepath.endswith('.tgz'):
            with tarfile.open(filepath, 'r:gz') as tar_ref:
                tar_ref.extractall(dest_dir)
        elif filepath.endswith('.tar.zst'):
             # Requires zstandard library: pip install zstandard
            import zstandard as zstd
            with open(filepath, 'rb') as f:
                dctx = zstd.ZstdDecompressor()
                with dctx.stream_reader(f) as reader:
                    with tarfile.open(fileobj=reader) as tar_ref:
                        tar_ref.extractall(path=dest_dir)
        else:
            print(f"   Unsupported archive format for {filename}", file=sys.stderr)
            return False
        print(f"   ✅ Unpacked successfully.")
        return True
    except Exception as e:
        print(f"   ❌ Error unpacking {filepath}: {e}", file=sys.stderr)
        return False

def main():
    parser = argparse.ArgumentParser(description="Download and set up project data.")
    parser.add_argument("--manifest", required=True, help="Path to the manifest JSON file.")
    parser.add_argument("--output-dir", required=True, help="Directory to download files to.")
    parser.add_argument("--profile", default="raw", help="Data profile to download (e.g., 'raw', 'processed'). Defaults to 'raw'.")
    args = parser.parse_args()

    if not os.path.exists(args.manifest):
        print(f"❌ Error: Manifest file not found at {args.manifest}", file=sys.stderr)
        sys.exit(1)

    with open(args.manifest, 'r') as f:
        manifest = json.load(f)

    base_url = manifest['base_url'].format(release_tag=manifest['release_tag'])
    profile_assets = manifest['profiles'].get(args.profile, [])
    
    if not profile_assets:
        print(f"❌ Error: Profile '{args.profile}' not found or is empty in the manifest.", file=sys.stderr)
        sys.exit(1)

    os.makedirs(args.output_dir, exist_ok=True)
    print(f"--- Starting data setup for profile: '{args.profile}' ---")

    for asset_item in profile_assets:
        asset_name = asset_item if isinstance(asset_item, str) else asset_item.get('name')
        if not asset_name:
            continue

        print(f"\nProcessing asset: {asset_name}")
        asset_info = manifest['assets'].get(asset_name)
        if not asset_info:
            print(f"⚠️  Warning: No asset details found for '{asset_name}' in manifest. Skipping.", file=sys.stderr)
            continue

        url = f"{base_url}/{asset_name}"
        download_path = os.path.join(args.output_dir, asset_name)
        expected_sha256 = asset_info.get('sha256')

        if verify_file(download_path, expected_sha256):
            print(f"   File already exists and is verified. Skipping download.")
        else:
            if os.path.exists(download_path):
                os.remove(download_path) # Remove corrupted file
            if not download_file(url, download_path):
                continue # Skip to next file on download failure
            
            if not verify_file(download_path, expected_sha256):
                print(f"   ❌ Downloaded file is corrupt. Please try again.", file=sys.stderr)
                continue

        unpack_type = asset_info.get('unpack')
        if unpack_type:
            dest_dir = asset_info.get('dest', '.')
            os.makedirs(dest_dir, exist_ok=True)
            unpack_archive(download_path, dest_dir)

    print("\n--- Data setup complete. ---")

if __name__ == "__main__":
    main()
