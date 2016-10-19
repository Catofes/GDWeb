from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage
from Utils.config import RConfig
from Utils.database import RDateBasePool
import argparse, uuid


def generate_credentials(id, secret):
    path = RConfig().work_dir + RConfig().credential_path
    output = str(uuid.uuid4())
    output_file = path + "credential/" + output + ".json"
    secret_file = path + "secret/" + secret
    #print(secret_file)
    store = Storage(output_file)
    credentials = store.get()
    if credentials and credentials.invalid:
        return
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args(['--noauth_local_webserver'])
    flow = client.flow_from_clientsecrets(secret_file, 'https://www.googleapis.com/auth/drive')
    flow.user_agent = "GDWeb"
    tools.run_flow(flow, store, flags)
    setting_file = path + "setting/" + output + ".yaml"
    setting_content = """client_config_file: %s
save_credentials: True
save_credentials_backend: file
save_credentials_file: %s
get_refresh_token: True
oauth_scope:
  - https://www.googleapis.com/auth/drive
""" % (secret_file, output_file)
    with open(setting_file, "w") as f:
        f.write(setting_content)
    RDateBasePool().execute("INSERT INTO auth(id, secret_file, credential_file, setting_file) VALUES (%s, %s, %s, %s);",
                            (id, secret_file, output_file, setting_file))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--secret", help="Google Api Secret File.")
    parser.add_argument("-i", "--id", help="Google User Id.")
    args = parser.parse_args()
    if not args.id or not args.secret:
        print("Missing Parameters.")
        exit(1)
    generate_credentials(args.id, args.secret)
