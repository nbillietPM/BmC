import os
from pathlib import Path
from getpass import getpass
from pygbif import occurrences

def verify_gbif_credentials(env_path=".env", overwrite=False):
    """
    Ensures GBIF credentials (GBIF_USER, GBIF_MAIL, GBIF_PWD) are stored in environment variables.
    If missing or invalid, prompts the user to enter them and stores them in a .env file for future use.

    Args:
        env_path (str): Path to the .env file.
        overwrite (bool): If True, forces user to re-enter credentials even if they exist.

    Returns:
        dict: A dictionary with keys 'GBIF_USER', 'GBIF_MAIL', 'GBIF_PWD'.
    """
    credentials = ["GBIF_USER", "GBIF_MAIL", "GBIF_PWD"]

    while True:
        # Prompt for credentials if missing or overwrite is True
        for cred in credentials:
            if overwrite or not os.environ.get(cred):
                if cred == "GBIF_PWD":
                    os.environ[cred] = getpass(f"Enter {cred}: ")
                else:
                    os.environ[cred] = input(f"Enter {cred}: ")

        user = os.environ.get("GBIF_USER")
        email = os.environ.get("GBIF_MAIL")
        pwd = os.environ.get("GBIF_PWD")

        # Verify credentials using pygbif
        try:
            result = occurrences.download_list(user=user, pwd=pwd)
            if isinstance(result, dict):
                print("✅ GBIF credentials are valid.")
                break
            else:
                print("⚠️ Unexpected response, please check your credentials.")
                for cred in credentials:
                    os.environ.pop(cred, None)
        except Exception as e:
            print("❌ Invalid credentials or error accessing GBIF API:", e)
            for cred in credentials:
                os.environ.pop(cred, None)

    # Save credentials in .env
    env_file = Path(env_path)
    existing_lines = []
    if env_file.exists():
        existing_lines = env_file.read_text().splitlines()

    for cred in credentials:
        value = os.environ[cred]
        replaced = False
        for i, line in enumerate(existing_lines):
            if line.startswith(f"{cred}="):
                existing_lines[i] = f"{cred}={value}"
                replaced = True
        if not replaced:
            existing_lines.append(f"{cred}={value}")

    env_file.write_text("\n".join(existing_lines) + "\n")
    print(f"Credentials stored in {env_file.resolve()}.")

    return {cred: os.environ[cred] for cred in credentials}