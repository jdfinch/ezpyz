

def read_paginated(file_path, chunk_size=1024 * 1024):  # 1 MB by default
    with open(file_path, 'r') as file:
        while chunk := file.read(chunk_size):
            yield chunk  # Process the chunk of data

# Example usage
for chunk in read_paginated('README.md', chunk_size=16):
    print(chunk, '||')  # Process each chunk of data
