import pandas as pd
import zstandard as zstd
import hashlib
import os

# --- Step 1: Data Generation ---
# This function generates a synthetic log file to simulate a high-frequency data stream.
# The data is designed to have intentional redundancy and patterns.
def generate_log_data(file_path, num_entries):
    print(f"Generating {num_entries} synthetic log entries...")
    with open(file_path, 'w') as f:
        for i in range(num_entries):
            # Create a timestamp to simulate real-world data
            timestamp = pd.Timestamp.now() + pd.Timedelta(seconds=i)
            # Create a message with a pattern
            message_index = i % len(log_messages)
            log_entry = f"{timestamp.isoformat()},{log_messages[message_index]}\n"
            f.write(log_entry)
    print("Data generation complete.")

# --- Step 2: Aggregation and Summarization ---
# This function uses a sliding window to aggregate data, mimicking real-time summarization.
def aggregate_data(file_path, output_path):
    print("Performing data aggregation...")
    df = pd.read_csv(file_path, header=None, names=['timestamp', 'message'])
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Use a sliding window of one minute to aggregate
    df.set_index('timestamp', inplace=True)
    aggregated_df = df.groupby(pd.Grouper(freq='1min')).agg(
        total_logs=pd.NamedAgg(column='message', aggfunc='count'),
        unique_messages=pd.NamedAgg(column='message', aggfunc='nunique')
    )
    
    aggregated_df.to_csv(output_path)
    print("Aggregation complete.")
    return aggregated_df.shape

# --- Step 3: Deduplication ---
# This function performs a simple hash-based deduplication on a file.
def deduplicate_data(file_path, output_path):
    print("Performing data deduplication...")
    unique_hashes = set()
    unique_lines =
    
    with open(file_path, 'r') as f:
        for line in f:
            # Create a hash of the line to identify duplicates
            line_hash = hashlib.sha256(line.encode('utf-8')).hexdigest()
            if line_hash not in unique_hashes:
                unique_hashes.add(line_hash)
                unique_lines.append(line)
                
    with open(output_path, 'w') as f:
        f.writelines(unique_lines)
    
    print("Deduplication complete.")
    return len(unique_lines)

# --- Step 4: Compression ---
# This function compresses a file using the zstandard library.
def compress_file(file_path, output_path):
    print("Performing Zstandard compression...")
    cctx = zstd.ZstdCompressor(level=10)
    with open(file_path, 'rb') as f_in, open(output_path, 'wb') as f_out:
        f_out.write(cctx.compress(f_in.read()))
    print("Compression complete.")

# --- Main Execution Flow ---
if __name__ == "__main__":
    # Define file paths and parameters
    initial_file = 'initial_data.csv'
    aggregated_file = 'aggregated_data.csv'
    deduplicated_file = 'deduplicated_data.csv'
    compressed_file = 'final_data.zst'
    num_entries = 10000
    
    # 1. Generate initial data
    generate_log_data(initial_file, num_entries)
    initial_size = os.path.getsize(initial_file)
    print(f"Initial raw data size: {initial_size} bytes")
    
    # 2. Aggregate data
    aggregate_data(initial_file, aggregated_file)
    aggregated_size = os.path.getsize(aggregated_file)
    print(f"Size after aggregation: {aggregated_size} bytes")
    
    # 3. Deduplicate data
    deduplicate_data(aggregated_file, deduplicated_file)
    deduplicated_size = os.path.getsize(deduplicated_file)
    print(f"Size after deduplication: {deduplicated_size} bytes")
    
    # 4. Compress data
    compress_file(deduplicated_file, compressed_file)
    compressed_size = os.path.getsize(compressed_file)
    print(f"Final compressed size: {compressed_size} bytes")
    
    # --- Print Summary of Results ---
    print("\n--- Summary of Results ---")
    print(f"Initial raw size: {initial_size} bytes")
    print(f"Size after aggregation: {aggregated_size} bytes ({100 - (aggregated_size/initial_size)*100:.2f}% reduction)")
    print(f"Size after deduplication: {deduplicated_size} bytes ({100 - (deduplicated_size/aggregated_size)*100:.2f}% reduction from aggregated)")
    print(f"Final compressed size: {compressed_size} bytes ({100 - (compressed_size/initial_size)*100:.2f}% total reduction)")
    
    # Clean up generated files
    os.remove(initial_file)
    os.remove(aggregated_file)
    os.remove(deduplicated_file)
    os.remove(compressed_file)
    print("\nTemporary files have been removed.")