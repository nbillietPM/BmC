import subprocess
import re

def generate_clean_requirements(output_file="requirements.txt"):
    # Run pip freeze and capture the output
    result = subprocess.run(['pip', 'freeze'], capture_output=True, text=True)
    raw_reqs = result.stdout.splitlines()

    clean_reqs = []
    
    for line in raw_reqs:
        # 1. Skip your own local editable package (-e .)
        if line.startswith('-e') or line.startswith('src') or line.startswith('my_cube_project'):
            continue
            
        # 2. Fix the Conda "@ file://" pathing issue
        # This strips the local path and just keeps the package name
        if ' @ ' in line:
            package_name = line.split(' @ ')[0]
            clean_reqs.append(package_name)
            
        # 3. Skip Conda packages that pip cannot resolve
        elif 'conda' in line.lower() or 'mkl' in line.lower():
            continue
            
        # 4. Keep standard pip packages (e.g., numpy==1.24.3)
        else:
            clean_reqs.append(line)

    # Sort alphabetically for cleanliness
    clean_reqs.sort()

    # Write to the file
    with open(output_file, 'w') as f:
        f.write('\n'.join(clean_reqs) + '\n')
        
    print(f"Successfully generated clean {output_file} with {len(clean_reqs)} packages.")

if __name__ == "__main__":
    generate_clean_requirements()