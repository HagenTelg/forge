import typing
import re
import logging
import subprocess
from concurrent.futures import ThreadPoolExecutor, Future
from smtplib import SMTP, SMTPNotSupportedError
from dns.exception import DNSException
from dns.resolver import resolve
from email.message import EmailMessage
from email.headerregistry import AddressHeader
from socket import getfqdn
from dynaconf import Dynaconf

_LOGGER = logging.getLogger(__name__)

valid_email = re.compile(r"[^@]+@[^@]+")

_executor = ThreadPoolExecutor(thread_name_prefix="EmailSend")


def is_valid_email(email: str) -> bool:
    if len(email) > 255:
        return False
    return valid_email.fullmatch(email) is not None


def send_email(message: EmailMessage, relay: typing.Optional[typing.Union[dict, Dynaconf]] = None) -> Future:
    relay_host = None
    relay_port = 0
    relay_user = None
    relay_password = None
    tls = None
    sendmail_command = None
    if relay is not None:
        if isinstance(relay, str):
            relay_host = relay
        else:
            relay_host = relay.get('RELAY', None)
            relay_port = int(relay.get('PORT', 0))
            relay_user = relay.get('USER')
            relay_password = relay.get('PASSWORD')
            sendmail_command = relay.get('COMMAND')
            tls = relay.get('TLS')
            if 'From' not in message:
                default_from = relay.get('FROM')
                if default_from is not None:
                    message['From'] = default_from

    def recipients_from_header(header: typing.Optional[AddressHeader], hosts=False) -> typing.Iterable[str]:
        if not header:
            return []
        result = []
        for addr in header.addresses:
            if hosts:
                result.append(addr.domain)
            else:
                result.append(f'{addr.username}@{addr.domain}')
        return result

    def send_relay():
        if 'From' not in message:
            message['From'] = "Forge Data System <forge@" + getfqdn() + ">"

        s = SMTP()
        try:
            s.connect(relay_host, relay_port)
            if tls is None or tls:
                try:
                    s.starttls()
                except SMTPNotSupportedError:
                    pass
            if relay_user is not None:
                s.login(relay_user, relay_password or "")
            s.send_message(message)
            s.quit()
        except Exception:
            _LOGGER.warning(f"Error sending email", exc_info=True)

    def send_direct():
        if 'From' not in message:
            message['From'] = "Forge Data System <forge@" + getfqdn() + ">"

        unique_hosts = set(recipients_from_header(message['To'], hosts=True))
        unique_hosts = unique_hosts.union(recipients_from_header(message['CC'], hosts=True))

        for host in unique_hosts:
            s = SMTP()
            try:
                try:
                    records = resolve(host, 'MX')
                    if len(records) == 0:
                        raise DNSException
                    s.connect(str(records[0].exchange))
                except DNSException:
                    s.connect(host)
                try:
                    s.starttls()
                except SMTPNotSupportedError:
                    pass
                s.send_message(message)
                s.quit()
            except Exception:
                _LOGGER.warning(f"Error sending email on {host}", exc_info=True)

    def send_command():
        if 'From' not in message:
            message['From'] = "Forge Data System <forge@" + getfqdn() + ">"

        unique_recipients = set(recipients_from_header(message['To']))
        unique_recipients = unique_recipients.union(recipients_from_header(message['CC']))

        args = list()
        if isinstance(sendmail_command, str):
            args.append(sendmail_command)
        else:
            args.extend(sendmail_command)
        args.extend(unique_recipients)

        try:
            p = subprocess.run(args, stdout=subprocess.DEVNULL, encoding='utf-8',
                               input=message.as_string(unixfrom=True))
        except:
            _LOGGER.warning(f"Error running sendmail command", exc_info=True)
            return
        if p.returncode != 0:
            _LOGGER.warning(f"Sendmail command exited with status {p.returncode}")

    if relay_host is not None:
        return _executor.submit(send_relay)
    elif sendmail_command is not None:
        return _executor.submit(send_command)
    else:
        return _executor.submit(send_direct)
