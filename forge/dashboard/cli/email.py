import typing
import asyncio
import logging
import time
from math import floor
from concurrent.futures import Future
from forge.dashboard import CONFIGURATION
from forge.emailutil import send_email, EmailMessage
from .interface import ControlInterface, Severity
from .display import sort_entries


_LOGGER = logging.getLogger(__name__)


async def send_entry_emails(interface: ControlInterface, args) -> None:
    from forge.vis.access.database import EmailInterface
    from forge.vis.dashboard.assemble import get_record

    access_database_uri = args.access_database_uri
    if access_database_uri is None:
        access_database_uri = CONFIGURATION.AUTHENTICATION.DATABASE
    email_interface = EmailInterface(access_database_uri)

    start_epoch = time.time() - float(args.interval) * 24 * 60 * 60

    entries = await interface.list_filtered(**vars(args))
    sort_keys = args.sort.split(',')
    if len(sort_keys) <= 0 or len(sort_keys[0]) <= 0:
        sort_keys = []
    sort_entries(sort_keys, entries)

    email_futures: typing.List[Future] = list()
    for e in entries:
        station = e['station']
        code = e['code']

        record = get_record(station, code)
        if not record:
            _LOGGER.debug(f"No record available for {(station or '').upper()}/{code}, email skipped")
            continue

        contents = await record.email(db=interface, station=station, entry_code=code, resend=args.resend,
                                      start_epoch_ms=int(floor(start_epoch * 1000.0)))
        if not contents:
            _LOGGER.debug(f"No email for {(station or '').upper()}/{code}")
            continue

        severity = contents.severity
        if contents.entry.status.abnormal:
            severity = Severity.ERROR

        recipients = await email_interface.get_recipients(station, code, severity)
        recipients.update(contents.send_to)
        if not recipients:
            _LOGGER.debug(f"No recipients for {(station or '').upper()}/{code} - {severity.name if severity else 'OK'}")
            continue

        _LOGGER.info(f"Sending email for {(station or '').upper()}/{code} - {severity.name if severity else 'OK'} to {len(recipients)} recipient(s)")

        recipients = list(recipients)
        recipients.sort()
        reply_to = contents.reply_to
        subject = contents.subject
        if contents.expose_all_recipients:
            message = EmailMessage()
            message['Subject'] = subject
            message['To'] = ', '.join(recipients)
            if reply_to:
                reply_to.update(recipients)
                reply_to = list(reply_to)
                reply_to.sort()
                reply_to = ', '.join(reply_to)
                message['Reply-To'] = reply_to
            message.set_content(contents.text)
            if contents.html:
                message.add_alternative(contents.html, subtype='html')
            email_futures.append(send_email(message, CONFIGURATION.get('EMAIL')))
        else:
            if reply_to:
                reply_to = list(reply_to)
                reply_to.sort()
                reply_to = ', '.join(reply_to)
            for r in recipients:
                message = EmailMessage()
                message['Subject'] = subject
                message['To'] = r
                if reply_to:
                    message['Reply-To'] = reply_to
                message.set_content(contents.text)
                if contents.html:
                    message.add_alternative(contents.html, subtype='html')
                email_futures.append(send_email(message, CONFIGURATION.get('EMAIL')))

    if len(email_futures) > 0:
        _LOGGER.debug(f"Waiting for completion of {len(email_futures)} email(s)")
        await asyncio.wait([asyncio.wrap_future(f) for f in email_futures])
    _LOGGER.info("Email send complete")


async def output_email_contents(interface: ControlInterface, args) -> None:
    from forge.vis.dashboard.assemble import get_record

    station = args.email_station
    if station == '_':
        station = None
    if not station:
        station = None
    else:
        station = station.lower()
    code = args.email_code.lower()

    record = get_record(station, code)
    if not record:
        _LOGGER.error(f"No record available for {(station or '').upper()}/{code}")
        exit(1)

    start_epoch = time.time() - float(args.interval) * 24 * 60 * 60

    contents = await record.email(db=interface, station=station, entry_code=code, resend=True,
                                  start_epoch_ms=int(floor(start_epoch * 1000.0)))
    if not contents:
        _LOGGER.error(f"No email available for {(station or '').upper()}/{code}")
        exit(1)

    if args.html:
        if not contents.html:
            _LOGGER.error(f"No HTML available for {(station or '').upper()}/{code}")
            exit(1)
        print(contents.html)
    else:
        print(contents.text)
