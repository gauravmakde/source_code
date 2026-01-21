import argparse
import json
import logging
import os
import smtplib

from vault_client import Vault


def __fill_args_placeholders(args):
    result = vars(args)

    if args.replace_mapping is None:
        return result

    replace_mapping = json.loads(result.pop('replace_mapping'))

    for key, value in result.items():
        for replace_key, replace_val in replace_mapping.items():
            value = value.replace(f'<{replace_key}>', replace_val)
        result[key] = value

    return result


def __get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--vault_mount_point", type=str, default="nordsecrets")
    parser.add_argument("--vault_host_env_var", type=str, default="VAULT_PATH")
    parser.add_argument("--vault_port_env_var", type=str, default="VAULT_PORT")
    parser.add_argument("--k8s_vault_role_id_env_var", type=str, default="VAULT_ROLE_ID")
    parser.add_argument("--k8s_vault_secret_id_env_var", type=str, default="VAULT_SECRET")
    parser.add_argument("--vault_email_key_env_var", type=str, default="MAIL_SA_VAULT_PATH")
    parser.add_argument("--vault_email_value_user_key", type=str, default="user")
    parser.add_argument("--vault_email_value_password_key", type=str, default="password")
    parser.add_argument("--mail_subject", type=str, required=True)
    parser.add_argument("--mail_body", type=str, required=True)
    parser.add_argument("--from_address", type=str, required=True)
    parser.add_argument("--to_addresses", type=str, required=True)
    parser.add_argument("--smtp_server", type=str, default="exchsmtp.nordstrom.net")
    parser.add_argument("--smtp_port", type=str, default="25")
    parser.add_argument("--replace_mapping", type=str, required=False)
    args = parser.parse_args()

    return __fill_args_placeholders(args=args)


def __env_var(key):
    return os.environ.get(key)


def __get_vault_host(args):
    return f"https://{__env_var(args['vault_host_env_var'])}:{__env_var(args['vault_port_env_var'])}"


def main():
    args = __get_arguments()

    vault = Vault(
        __get_vault_host(args=args),
        __env_var(args['k8s_vault_role_id_env_var']),
        __env_var(args['k8s_vault_secret_id_env_var'])
    )

    sa_creds = vault.fetch_secrets(__env_var(args['vault_email_key_env_var']), args['vault_mount_point'])
    sa_username = sa_creds.get('data').get('data').get(args['vault_email_value_user_key'])
    sa_password = sa_creds.get('data').get('data').get(args['vault_email_value_password_key'])
    [mail.strip() for mail in str.split(",")]
    to_addresses = [mail_to.strip() for mail_to in args['to_addresses'].split(',')]

    with smtplib.SMTP(host=args['smtp_server'], port=args['smtp_port']) as smtp:
        smtp.connect(args['smtp_server'], args['smtp_port'])
        smtp.starttls()
        smtp.login(user=sa_username, password=sa_password)
        smtp.sendmail(
            from_addr=args['from_address'].strip(),
            to_addrs=to_addresses,
            msg=f'Subject: {args["mail_subject"]}\n\n{args["mail_body"]}'
        )
        smtp.quit()


if __name__ == '__main__':
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    main()
