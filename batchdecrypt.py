import os
import subprocess
import requests
import shutil
import glob
import time
import re
from pathlib import Path
import concurrent.futures

# Configuration
input_dir = "inputfiles"
base_dir = "processing"
max_workers = 50

def find_tasks():
    """Find all playlist/key pairs with custom filename format"""
    tasks = []
    for m3u8_path in glob.glob(os.path.join(input_dir, "*.m3u8")):
        filename = os.path.basename(m3u8_path)
        
        # Extract base name and version using regex
        match = re.match(r'^(.+?)(\d+\.\d+)\.m3u8$', filename)
        if not match:
            print(f"Skipping invalid filename format: {filename}")
            continue
            
        base_name = match.group(1)
        version = match.group(2)
        key_filename = f"{base_name}{version}.bin"
        key_path = os.path.join(input_dir, key_filename)
        
        if not os.path.exists(key_path):
            raise FileNotFoundError(f"Missing key file: {key_filename}")

        tasks.append({
            "base_name": base_name,
            "version": version,
            "output_file": f"{base_name} {version}.mp4",
            "playlist": m3u8_path,
            "key": key_path,
            "downloaded_dir": os.path.join(base_dir, f"downloaded_{version}"),
            "decrypted_dir": os.path.join(base_dir, f"decrypted_{version}")
        })
    
    # Sort tasks by version numbers
    return sorted(tasks, key=lambda x: [int(n) for n in x["version"].split('.')])

def parse_m3u8(playlist_path):
    """Extract IV and segments from m3u8 file"""
    iv = None
    segments = []
    
    with open(playlist_path, "r") as f:
        for line in f:
            line = line.strip()
            if line.startswith("#EXT-X-KEY"):
                attributes = line.split(":", 1)[1].split(",")
                for attr in attributes:
                    if attr.startswith("IV="):
                        iv = attr.split("=", 1)[1].strip('"')
                        if iv.lower().startswith("0x"):
                            iv = iv[2:]
            elif ".ts?" in line:
                segments.append(line)
    
    if not iv:
        raise ValueError(f"IV not found in {playlist_path}")
    
    return iv, segments

def download_segment(task, i, url):
    """Download a single segment"""
    ts_file = os.path.join(task["downloaded_dir"], f"segment{i}.ts")
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    with open(ts_file, "wb") as f:
        f.write(response.content)

def process_task(task):
    """Process a single playlist/key pair"""
    print(f"\nProcessing {task['base_name']} {task['version']}")
    
    try:
        # Create directories
        Path(task["downloaded_dir"]).mkdir(parents=True, exist_ok=True)
        Path(task["decrypted_dir"]).mkdir(parents=True, exist_ok=True)

        # Read key
        with open(task["key"], "rb") as f:
            key_hex = f.read().hex()

        # Parse playlist
        iv, segments = parse_m3u8(task["playlist"])

        # Download segments
        print(f"Downloading {len(segments)} segments...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(download_segment, task, i, url) 
                     for i, url in enumerate(segments)]
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Download error : {e}")

        # Decrypt segments
        print("Decrypting segments...")
        for i in range(len(segments)):
            input_ts = os.path.join(task["downloaded_dir"], f"segment{i}.ts")
            output_ts = os.path.join(task["decrypted_dir"], f"segment{i}.ts")
            
            subprocess.run([
                "openssl", "aes-128-cbc", "-d",
                "-in", input_ts,
                "-out", output_ts,
                "-nosalt",
                "-iv", iv,
                "-K", key_hex
            ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        # Create file list
        task_file_list = os.path.join(task["decrypted_dir"], "filelist.txt")
        segments = sorted(
            glob.glob(os.path.join(task["decrypted_dir"], "*.ts")),
            key=lambda x: int(os.path.basename(x).split("segment")[1].split(".")[0]))
        
        with open(task_file_list, "w") as f:
            for seg in segments:
                f.write(f"file '{os.path.basename(seg)}'\n")

        # Create output file
        print(f"Creating {task['output_file']}")
        subprocess.run([
            "ffmpeg",
            "-hide_banner",
            "-loglevel", "error",
            "-f", "concat",
            "-safe", "0",
            "-i", task_file_list,
            "-c", "copy",
            task["output_file"]
        ], check=True)

        return task["output_file"]

    finally:
        time.sleep(0.5)

def main():
    """Main function with proper cleanup"""
    try:
        # Check input directory
        if not os.path.exists(input_dir):
            raise FileNotFoundError(f"Input directory '{input_dir}' not found")

        # Prepare processing environment
        shutil.rmtree(base_dir, ignore_errors=True)
        Path(base_dir).mkdir(exist_ok=True)

        tasks = find_tasks()
        if not tasks:
            raise ValueError(f"No valid playlist/key pairs found in {input_dir}")

        # Process all tasks
        output_files = []
        for task in tasks:
            try:
                output_file = process_task(task)
                output_files.append(output_file)
            except Exception as e:
                print(f"Failed to process {task['base_name']} {task['version']}: {e}")

        # Output results
        print("\nSuccessfully created files:")
        for file in output_files:
            if os.path.exists(file):
                file_path = Path(file).resolve()
                print(f"- {file_path} ({file_path.stat().st_size / 1024 / 1024:.2f} MB)")
                
    finally:
        # Final cleanup
        print("\nPerforming final cleanup...")
        time.sleep(1)
        if os.path.exists(base_dir):
            shutil.rmtree(base_dir, ignore_errors=True)
        print("Cleanup completed")

if __name__ == "__main__":
    main()