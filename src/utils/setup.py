"""Set env variables."""
import configparser as ConfigParser
import os

region = os.getenv("N26_REGION").upper()

config_ini = ConfigParser.ConfigParser()
config_file = f"/app/utils/CONFIG.ini"
config_ini.read(config_file)

vault_section = f"local-{region}"
