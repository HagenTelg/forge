import typing
import logging
import asyncio
import datetime
import random
import sqlalchemy as db
import sqlalchemy.orm as orm
import starlette.status
from secrets import token_urlsafe
from concurrent.futures import Future
from starlette.responses import Response, RedirectResponse, JSONResponse, HTMLResponse
from starlette.exceptions import HTTPException
from starlette.authentication import requires
from starlette.routing import Route
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from passlib.hash import pbkdf2_sha256
from authlib.integrations.starlette_client import OAuth
from forge.emailutil import is_valid_email, send_email, EmailMessage
from forge.tasks import background_task
from forge.vis.util import package_template, name_to_initials
from forge.vis import CONFIGURATION
from forge.const import DISPLAY_STATIONS
from forge.database import ORMDatabase
from . import BaseAccessUser, BaseAccessController, Request


_LOGGER = logging.getLogger(__name__)

_Base = orm.declarative_base()


class _User(_Base):
    __tablename__ = 'users'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mariadb_engine': 'InnoDB',
    }

    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(255), index=True)
    name = db.Column(db.Unicode(255), nullable=True)
    initials = db.Column(db.Unicode(255), nullable=True)
    last_seen = db.Column(db.DateTime, nullable=True)


class _Session(_Base):
    __tablename__ = 'sessions'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mariadb_engine': 'InnoDB',
    }

    id = db.Column(db.Integer, primary_key=True)
    user = db.Column(db.Integer, db.ForeignKey('users.id', ondelete='CASCADE'), index=True)
    token = db.Column(db.String(43), index=True)  # Base64 encoding
    last_seen = db.Column(db.DateTime)

    _users = orm.relationship(_User, backref=orm.backref('sessions', passive_deletes=True))


class _Access(_Base):
    __tablename__ = 'access'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mariadb_engine': 'InnoDB',
    }

    id = db.Column(db.Integer, primary_key=True)
    user = db.Column(db.Integer, db.ForeignKey('users.id', ondelete='CASCADE'), index=True)
    station = db.Column(db.String(32))
    mode = db.Column(db.String(128))
    write = db.Column(db.Boolean)

    _users = orm.relationship(_User, backref=orm.backref('access', passive_deletes=True))


class _AccessChallenge(_Base):
    __tablename__ = 'access_challenge'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mariadb_engine': 'InnoDB',
    }

    id = db.Column(db.Integer, primary_key=True)
    token = db.Column(db.String(43), index=True)  # Base64 encoding
    user = db.Column(db.Integer, db.ForeignKey('users.id', ondelete='CASCADE'))
    station = db.Column(db.String(32))
    mode = db.Column(db.String(128))
    write = db.Column(db.Boolean)
    valid_until = db.Column(db.DateTime)

    _users = orm.relationship(_User, backref=orm.backref('access_challenge', passive_deletes=True))


class _AuthPassword(_Base):
    __tablename__ = 'auth_password'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mariadb_engine': 'InnoDB',
    }

    user = db.Column(db.Integer, db.ForeignKey('users.id', ondelete='CASCADE'), primary_key=True)
    pbkdf2 = db.Column(db.String(255))

    _users = orm.relationship(_User, backref=orm.backref('auth_password', passive_deletes=True))


class _PasswordResetChallenge(_Base):
    __tablename__ = 'password_reset'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mariadb_engine': 'InnoDB',
    }

    token = db.Column(db.String(43), primary_key=True)  # Base64 encoding
    user = db.Column(db.Integer, db.ForeignKey('users.id', ondelete='CASCADE'))
    valid_until = db.Column(db.DateTime)

    _users = orm.relationship(_User, backref=orm.backref('password_reset', passive_deletes=True))


class _AuthOpenIDConnect(_Base):
    __tablename__ = 'auth_oidc'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mariadb_engine': 'InnoDB',
    }

    user = db.Column(db.Integer, db.ForeignKey('users.id', ondelete='CASCADE'), primary_key=True)
    provider = db.Column(db.String(32), index=True)
    sub = db.Column(db.String(255), index=True)

    _users = orm.relationship(_User, backref=orm.backref('auth_oidc', passive_deletes=True))


class AccessController(BaseAccessController):
    def __init__(self, uri: str):
        self.db = ORMDatabase(uri, _Base)
        self._session_purge_started = False

        self.routes: typing.List[Route] = [
            Route('/login', endpoint=self.login, name='login'),
            Route('/logout', endpoint=self.logout, methods=['GET'], name='logout'),
            Route('/change_info', endpoint=self.info_change, methods=['GET', 'POST'], name='change_user_info'),
            Route('/request', endpoint=self.request_access, methods=['GET', 'POST'], name='request_access'),
            Route('/confirm', endpoint=self.confirm_access, methods=['GET'], name='confirm_access'),
            Route('/password/login', endpoint=self.password_login, methods=['POST'], name='login_password'),
            Route('/password/change', endpoint=self.password_change, methods=['POST'], name='change_password'),
            Route('/password/reset_issue', endpoint=self.password_reset_challenge, methods=['POST'], name='reset_password_send'),
            Route('/password/reset', endpoint=self.password_reset_response, methods=['GET'], name='reset_password'),
            Route('/password/create', endpoint=self.password_create_user, methods=['POST'], name='create_password'),
        ]

        self.oauth = OAuth()

        self.enable_google = False
        if CONFIGURATION.exists('AUTHENTICATION.GOOGLE'):
            self.oauth.register(
                'google',
                client_id=CONFIGURATION.AUTHENTICATION.GOOGLE.CLIENT_ID,
                client_secret=CONFIGURATION.AUTHENTICATION.GOOGLE.CLIENT_SECRET,
                server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
                client_kwargs={'scope': 'openid profile email'}
            )
            self.routes.append(Route('/google/login', endpoint=self.google_login, methods=['GET'], 
                                     name='login_google'))
            self.routes.append(Route('/google/authorize', endpoint=self.google_authorize, 
                                     name='authorize_google'))
            self.enable_google = True

        self.enable_microsoft = False
        if CONFIGURATION.exists('AUTHENTICATION.MICROSOFT'):
            tenant = CONFIGURATION.get('CONFIGURATION.AUTHENTICATION.MICROSOFT.TENANT', 'consumers')
            self.oauth.register(
                'microsoft',
                client_id=CONFIGURATION.AUTHENTICATION.MICROSOFT.CLIENT_ID,
                client_secret=CONFIGURATION.AUTHENTICATION.MICROSOFT.CLIENT_SECRET,
                server_metadata_url=f'https://login.microsoftonline.com/{tenant}/v2.0/.well-known/openid-configuration',
                client_kwargs={'scope': 'openid profile email'}
            )
            self.routes.append(Route('/microsoft/login', endpoint=self.microsoft_login, methods=['GET'], 
                                     name='login_microsoft'))
            self.routes.append(Route('/microsoft/authorize', endpoint=self.microsoft_authorize, 
                                     name='authorize_microsoft'))
            self.enable_microsoft = True

        self.enable_yahoo = False
        if CONFIGURATION.exists('AUTHENTICATION.YAHOO'):
            self.oauth.register(
                'yahoo',
                client_id=CONFIGURATION.AUTHENTICATION.YAHOO.CLIENT_ID,
                client_secret=CONFIGURATION.AUTHENTICATION.YAHOO.CLIENT_SECRET,
                server_metadata_url='https://login.yahoo.com/.well-known/openid-configuration',
                client_kwargs={'scope': 'openid profile email'}
            )
            self.routes.append(Route('/yahoo/login', endpoint=self.yahoo_login, methods=['GET'],
                                     name='login_yahoo'))
            self.routes.append(Route('/yahoo/authorize', endpoint=self.yahoo_authorize,
                                     name='authorize_yahoo'))
            self.enable_yahoo = True

        self.enable_apple = False
        if CONFIGURATION.exists('AUTHENTICATION.APPLE'):
            self.oauth.register(
                'apple',
                client_id=CONFIGURATION.AUTHENTICATION.APPLE.CLIENT_ID,
                client_secret=CONFIGURATION.AUTHENTICATION.APPLE.CLIENT_SECRET,
                server_metadata_url='https://appleid.apple.com/.well-known/openid-configuration',
                client_kwargs={'scope': 'openid profile email'}
            )
            self.routes.append(Route('/apple/login', endpoint=self.apple_login, methods=['GET'],
                                     name='login_apple'))
            self.routes.append(Route('/apple/authorize', endpoint=self.apple_authorize,
                                     name='authorize_apple'))
            self.enable_apple = True

    def _purge_sessions(self) -> None:
        if self._session_purge_started:
            return
        self._session_purge_started = True

        async def purge():
            def remove_old_sessions(engine: Engine):
                # Session cookies last 14 days, so give it plenty of slack
                expire_before = datetime.datetime.utcnow() - datetime.timedelta(days=15)
                with Session(engine) as orm_session:
                    orm_session.query(_Session).filter(_Session.last_seen < expire_before).delete()
                    try:
                        orm_session.commit()
                    except:
                        return

                _LOGGER.debug(f"Sessions before {expire_before:%Y-%m-%d} removed")

            while True:
                self.db.background(remove_old_sessions)
                await asyncio.sleep(random.uniform(7200, 21600))

        background_task(purge())

    async def authenticate(self, request: Request) -> typing.Optional[BaseAccessUser]:
        self._purge_sessions()

        session_user_id = request.session.get('id')
        if session_user_id is None:
            return None
        session_token = request.session.get('token')
        if session_token is None:
            return None
        try:
            session_user_id = int(session_user_id)
            session_token = str(session_token)
        except ValueError:
            return None

        now = datetime.datetime.utcnow()

        def execute(engine: Engine):
            with Session(engine) as orm_session:
                session = orm_session.query(_Session).filter_by(user=session_user_id, token=session_token).one_or_none()
                if session is None:
                    return None

                user = orm_session.query(_User).filter_by(id=session_user_id).one_or_none()
                if user is None:
                    return None

                if (now - session.last_seen).total_seconds() > 3600:
                    user.last_seen = now
                    session.last_seen = now
                    try:
                        orm_session.commit()
                    except:
                        pass

                _LOGGER.debug(f"Found session token for '{user.email}' ({session_user_id})")
                return AccessUser(self, user)
        return await self.db.execute(execute)

    async def login(self, request: Request) -> Response:
        self._purge_sessions()

        return HTMLResponse(await package_template('access', 'login.html').render_async(
            request=request,
            enable_google=self.enable_google,
            enable_microsoft=self.enable_microsoft,
            enable_yahoo=self.enable_yahoo,
            enable_apple=self.enable_apple,
        ))

    async def _clear_session(self, request: Request):
        session_user_id = request.session.get('id')
        session_token = request.session.get('token')
        request.session.clear()

        def clear_token(engine: Engine):
            with Session(engine) as orm_session:
                orm_session.query(_Session).filter_by(user=session_user_id, token=session_token).delete()
                orm_session.commit()

            _LOGGER.debug(f"Cleared session token {session_user_id}")

        if session_user_id is not None and session_token is not None:
            self.db.background(clear_token)

    async def logout(self, request: Request) -> Response:
        await self._clear_session(request)
        return RedirectResponse(request.url_for('root'))

    async def password_login(self, request: Request) -> Response:
        self._purge_sessions()

        data = await request.form()
        email = str(data.get('email', '')).lower()
        if not is_valid_email(email):
            raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid email address")
        password = str(data.get('password', ''))
        if password is None or len(password) < 8:
            raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid password")

        await self._clear_session(request)

        found_user = False

        def execute(engine: Engine):
            nonlocal found_user
            with Session(engine) as orm_session:
                for user in orm_session.query(_User).filter_by(email=email):
                    auth_entry = orm_session.query(_AuthPassword).filter_by(user=user.id).one_or_none()
                    if auth_entry is None:
                        continue
                    if not pbkdf2_sha256.verify(password, auth_entry.pbkdf2):
                        continue

                    user.last_seen = datetime.datetime.utcnow()
                    session = _Session(user=user.id, token=token_urlsafe(32), last_seen=user.last_seen)
                    orm_session.add(session)

                    orm_session.commit()
                    request.session['id'] = session.user
                    request.session['token'] = session.token

                    _LOGGER.info(f"Logged in user '{user.email}' ({session.user}) via password authentication")
                    found_user = True

        await self.db.execute(execute)
        if not found_user:
            raise HTTPException(starlette.status.HTTP_401_UNAUTHORIZED, detail="Invalid login")
        return RedirectResponse(request.url_for("root"), status_code=starlette.status.HTTP_302_FOUND)

    @requires('authenticated')
    async def password_change(self, request: Request) -> Response:
        if not isinstance(request.user, AccessUser):
            raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Not using a dynamic login")

        data = await request.json()
        password = str(data.get('password', '')).lower()
        if password is None or len(password) < 8:
            raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid password")

        def execute(engine: Engine) -> Response:
            with Session(engine) as orm_session:
                auth_entry = orm_session.query(_AuthPassword).filter_by(user=request.user.auth_user.id).one_or_none()
                if auth_entry is None:
                    raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="No password entry found")

                auth_entry.pbkdf2 = pbkdf2_sha256.hash(password)
                orm_session.commit()
                _LOGGER.debug(f"Changed password for '{request.user.auth_user.email}' ({request.user.auth_user.id})")
                return JSONResponse({'status': 'ok'})

        return await self.db.execute(execute)

    async def password_reset_challenge(self, request: Request) -> Response:
        data = await request.form()
        email = str(data.get('email', '')).lower()
        if not is_valid_email(email):
            raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid email address")

        challenge_token = token_urlsafe(32)
        challenge_created = False

        def execute(engine: Engine):
            nonlocal challenge_token
            nonlocal challenge_created

            now = datetime.datetime.utcnow()
            valid_until = now + datetime.timedelta(minutes=30)

            any_hit = False
            with Session(engine) as orm_session:
                orm_session.query(_PasswordResetChallenge).filter(_PasswordResetChallenge.valid_until < now).delete()

                for user in orm_session.query(_User).filter_by(email=email):
                    auth_entry = orm_session.query(_AuthPassword).filter_by(user=user.id).one_or_none()
                    if auth_entry is None:
                        continue
                    any_hit = True
                    challenge = _PasswordResetChallenge(user=user.id, token=challenge_token, valid_until=valid_until)
                    orm_session.add(challenge)

                orm_session.commit()
                challenge_created = any_hit

        await self.db.execute(execute)

        if challenge_created:
            _LOGGER.debug(f"Starting password reset challenge")

            template_context = {
                'request': request,
                'email': email,
                'reset_url': request.url_for('reset_password') + f'?token={challenge_token}',
            }

            message = EmailMessage()
            message['Subject'] = "Forge Visualization Tool Password Reset"
            message['To'] = email
            message.set_content(await package_template(
                'access', 'password_reset_email.txt').render_async(template_context))
            message.add_alternative(await package_template(
                'access', 'password_reset_email.html').render_async(template_context), subtype='html')
            send_email(message, CONFIGURATION.get('EMAIL'))

        return HTMLResponse(await package_template('access', 'password_reset_challenge.html').render_async(
            request=request,
            email=email,
        ))

    async def password_reset_response(self, request: Request) -> Response:
        reset_token = request.query_params.get('token')
        if reset_token is None:
            raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid reset request")

        request.session.clear()
        new_password = token_urlsafe(16)

        def execute(engine: Engine):
            added_session = None
            now = datetime.datetime.utcnow()
            with Session(engine) as orm_session:
                for challenge in orm_session.query(_PasswordResetChallenge).filter_by(token=reset_token).filter(
                        _PasswordResetChallenge.valid_until >= now):
                    auth_entry = orm_session.query(_AuthPassword).filter_by(user=challenge.user).one_or_none()
                    if auth_entry is None:
                        continue
                    orm_session.delete(challenge)
                    orm_session.query(_Session).filter_by(user=challenge.user).delete()

                    auth_entry.pbkdf2 = pbkdf2_sha256.hash(new_password)

                    _LOGGER.info(f"Reset password for user {challenge.user}")

                    if added_session is None:
                        added_session = _Session(user=challenge.user, token=token_urlsafe(32),
                                                 last_seen=now)
                        orm_session.add(added_session)

                orm_session.query(_PasswordResetChallenge).filter_by(token=reset_token).delete()
                orm_session.commit()
                if added_session is not None:
                    request.session['id'] = added_session.user
                    request.session['token'] = added_session.token

        await self.db.execute(execute)

        return HTMLResponse(await package_template("access", "password_reset_complete.html").render_async(
            request=request,
            new_password=new_password,
        ))

    async def password_create_user(self, request: Request) -> Response:
        data = await request.form()
        email = str(data.get('email', '')).lower()
        if not is_valid_email(email):
            raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid email address")
        password = str(data.get('password', ''))
        if password is None or len(password) < 8:
            raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid password")

        name = data.get('name', None)
        if name is not None:
            name = str(name)[0:255].strip()
            if len(name) == 0:
                name = None

        await self._clear_session(request)

        def execute(engine: Engine):
            with Session(engine) as orm_session:
                user = _User(email=email, name=name)
                if user.name is not None:
                    user.initials = name_to_initials(user.name)
                orm_session.add(user)
                orm_session.flush()

                auth = _AuthPassword(user=user.id, pbkdf2=pbkdf2_sha256.hash(password))
                orm_session.add(auth)
                session = _Session(user=user.id, token=token_urlsafe(32),
                                   last_seen=datetime.datetime.utcnow())
                orm_session.add(session)

                orm_session.commit()
                request.session['id'] = session.user
                request.session['token'] = session.token

                _LOGGER.info(f"Created password login for '{name}' {email} ({user.id})")

        await self.db.execute(execute)

        return RedirectResponse(request.url_for('root'), status_code=starlette.status.HTTP_302_FOUND)

    async def oidc_login_generic(self, request: Request, client_name: str, redirect_name: str):
        self._purge_sessions()

        client = self.oauth.create_client(client_name)
        redirect_uri = request.url_for(redirect_name)
        return await client.authorize_redirect(request, redirect_uri)

    async def oidc_authorize_generic(self, request: Request, client_name: str) -> Response:
        self._purge_sessions()

        client = self.oauth.create_client(client_name)
        token = await client.authorize_access_token(request)
        oidc_user = await client.parse_id_token(token, None)

        await self._clear_session(request)

        # Remove the suffixes NOAA policy requires
        def strip_noaa_suffixes(email: str, name: str) -> str:
            if not name:
                return name
            if not email.lower().endswith('@noaa.gov'):
                return name
            for suffix in ("- NOAA Federal", "- NOAA Affiliate"):
                if name.lower().endswith(suffix.lower()):
                    return name[:-len(suffix)].strip()
            return name

        def execute(engine: Engine):
            with Session(engine) as orm_session:
                auth = orm_session.query(_AuthOpenIDConnect).filter_by(provider=client_name,
                                                                       sub=oidc_user.sub).one_or_none()
                if auth is None:
                    email = oidc_user.get('email')
                    email = email[0:255] if email else ''
                    name = oidc_user.get('name')
                    name = strip_noaa_suffixes(email, name)
                    name = name[0:255] if name else None
                    user = _User(email=email, name=name, last_seen=datetime.datetime.utcnow())
                    if user.name is not None:
                        user.initials = name_to_initials(user.name)
                    orm_session.add(user)
                    orm_session.flush()

                    auth = _AuthOpenIDConnect(user=user.id, provider=client_name, sub=oidc_user.sub)
                    orm_session.add(auth)

                    _LOGGER.info(f"Created {client_name} login ({auth.sub}) for '{name}' {email} ({user.id})'")
                else:
                    user = orm_session.query(_User).filter_by(id=auth.user).one_or_none()
                    if user is None:
                        raise HTTPException(starlette.status.HTTP_500_INTERNAL_SERVER_ERROR, detail="No user found")
                    user.last_seen = datetime.datetime.utcnow()

                    _LOGGER.info(f"Logged in user '{user.name}' {user.email} ({user.id}) via {client_name} authentication ({auth.sub})")

                session = _Session(user=user.id, token=token_urlsafe(32),
                                   last_seen=datetime.datetime.utcnow())
                orm_session.add(session)

                orm_session.commit()
                request.session['id'] = session.user
                request.session['token'] = session.token

        await self.db.execute(execute)
        return RedirectResponse(request.url_for('root'), status_code=starlette.status.HTTP_302_FOUND)

    async def google_login(self, request: Request) -> Response:
        return await self.oidc_login_generic(request, 'google', 'authorize_google')

    async def google_authorize(self, request: Request) -> Response:
        return await self.oidc_authorize_generic(request, 'google')

    async def microsoft_login(self, request: Request) -> Response:
        return await self.oidc_login_generic(request, 'microsoft', 'authorize_microsoft')

    async def microsoft_authorize(self, request: Request) -> Response:
        return await self.oidc_authorize_generic(request, 'microsoft')

    async def yahoo_login(self, request: Request) -> Response:
        return await self.oidc_login_generic(request, 'yahoo', 'authorize_yahoo')

    async def yahoo_authorize(self, request: Request) -> Response:
        return await self.oidc_authorize_generic(request, 'yahoo')

    async def apple_login(self, request: Request) -> Response:
        return await self.oidc_login_generic(request, 'apple', 'authorize_apple')

    async def apple_authorize(self, request: Request) -> Response:
        return await self.oidc_authorize_generic(request, 'apple')

    @requires('authenticated')
    async def info_change(self, request: Request) -> Response:
        if not isinstance(request.user, AccessUser):
            raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Not using a dynamic login")

        if request.method.upper() == 'GET':
            def execute(engine: Engine):
                with Session(engine) as orm_session:
                    auth_entry = orm_session.query(_AuthPassword).filter_by(
                        user=request.user.auth_user.id).one_or_none()
                    if auth_entry is not None:
                        return True
                return False

            enable_password_change = self.db.execute(execute)

            return HTMLResponse(await package_template('access', 'user_info.html').render_async(
                request=request,
                enable_password_change=enable_password_change,
            ))

        data = await request.json()
        email = data.get('email')
        name = data.get('name')
        initials = data.get('initials')

        response = {'status': 'ok'}

        def execute(engine: Engine):
            nonlocal response
            nonlocal email
            nonlocal name
            nonlocal initials

            with Session(engine) as orm_session:
                user = orm_session.query(_User).filter_by(id=request.user.auth_user.id).one_or_none()
                if user is None:
                    raise HTTPException(starlette.status.HTTP_500_INTERNAL_SERVER_ERROR, detail="No user found")

                if email is not None:
                    email = str(email).lower()
                    email = email[0:255]
                    if is_valid_email(email):
                        user.email = email
                        response['email'] = user.email

                if name is not None:
                    name = str(name)
                    user.name = name[0:255]
                    response['name'] = user.name

                if initials is not None:
                    initials = str(initials)
                    user.initials = initials[0:255]
                    response['initials'] = user.initials

                orm_session.commit()

        await self.db.execute(execute)
        return JSONResponse(response)

    @requires('authenticated')
    async def request_access(self, request: Request) -> Response:
        if not isinstance(request.user, AccessUser):
            raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Not using a dynamic login")

        if request.method.upper() == 'GET':
            station = request.query_params.get("station")
            return HTMLResponse(await package_template('access', 'request.html').render_async(
                request=request,
                station=station,
            ))

        data = await request.form()
        station = data.get('station', '')[:32].lower()
        comment = data.get('comment', '')[:8192]

        _LOGGER.debug(f"Sending access request email")

        template_context = {
            'request': request,
            'station': station,
            'comment': comment,
            'user': request.user.auth_user
        }

        message = EmailMessage()
        message['Subject'] = "Forge Visualization Tool Access Request"
        addrs = ', '.join(CONFIGURATION.get('AUTHENTICATION.REQUEST.EMAIL', ["root@localhost"]))
        message['To'] = addrs
        message['Reply-To'] = addrs
        message.set_content(await package_template(
            'access', 'request_email.txt').render_async(template_context))
        message.add_alternative(await package_template(
            'access', 'request_email.html').render_async(template_context), subtype='html')
        send_email(message, CONFIGURATION.get('EMAIL'))

        return HTMLResponse(await package_template('access', 'request_submitted.html').render_async(
            request=request,
            station=station,
        ))

    @requires('authenticated')
    async def confirm_access(self, request: Request) -> Response:
        if not isinstance(request.user, AccessUser):
            raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Not using a dynamic login")

        confirm_token = request.query_params.get('token')
        if confirm_token is None:
            raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid access confirmation request")

        any_added = False

        def execute(engine: Engine):
            nonlocal any_added
            now = datetime.datetime.utcnow()
            with Session(engine) as orm_session:
                orm_session.query(_AccessChallenge).filter(_AccessChallenge.valid_until < now).delete()
                for challenge in orm_session.query(_AccessChallenge).filter_by(token=confirm_token,
                                                                               user=request.user.auth_user.id).filter(
                        _AccessChallenge.valid_until >= now):
                    orm_session.delete(challenge)
                    orm_session.add(_Access(user=challenge.user, station=challenge.station, mode=challenge.mode,
                                            write=challenge.write))

                    any_added = True
                    _LOGGER.info(f"Confirmed access for {challenge.user} on {challenge.station} - {challenge.mode}")

                orm_session.query(_AccessChallenge).filter_by(token=confirm_token).delete()
                orm_session.commit()

        await self.db.execute(execute)

        return HTMLResponse(await package_template("access", "request_confirmed.html").render_async(
            request=request,
            any_added=any_added,
        ))


class ControlInterface:
    def __init__(self, uri: str):
        self.db = ORMDatabase(uri, _Base)

    @staticmethod
    def _mode_filter(query, mode: str):
        if mode.endswith('*'):
            return query.filter(_Access.mode == mode)
        elif '%' in mode:
            return query.filter(_Access.mode.ilike(mode))
        else:
            return query.filter(_Access.mode.ilike(f'{mode}%'))

    @staticmethod
    def _select_users(orm_session: Session, **kwargs):
        def prepare_like(raw):
            if '*' in raw:
                return raw.replace('*', '%')
            return f'%{raw}%'

        def to_time(raw):
            if isinstance(raw, datetime.datetime):
                return raw
            now = datetime.datetime.utcnow()
            seconds = round(float(raw) * 86400)
            return now - datetime.timedelta(seconds=seconds)

        query = orm_session.query(_User)
        if kwargs.get('station') or kwargs.get('mode'):
            query = query.join(_Access)
            if kwargs.get('station'):
                query = query.filter(_Access.station == kwargs['station'].lower())
            if kwargs.get('mode'):
                query = ControlInterface._mode_filter(query, kwargs['mode'])
        if kwargs.get('user'):
            query = query.filter(_User.id == int(kwargs['user']))
        if kwargs.get('email'):
            query = query.filter(_User.email.ilike(prepare_like(kwargs['email'])))
        if kwargs.get('name'):
            query = query.filter(_User.name.ilike(prepare_like(kwargs['name'])))
        if kwargs.get('initials'):
            query = query.filter(_User.initials.ilike(prepare_like(kwargs['initials'])))
        if kwargs.get('before'):
            query = query.filter(_User.last_seen <= to_time(kwargs['before']))
        if kwargs.get('after'):
            query = query.filter(_User.last_seen >= to_time(kwargs['after']))
        if kwargs.get('never'):
            query = query.filter(_User.last_seen == None)
        return query

    async def list_users(self, **kwargs) -> typing.List[typing.Dict]:
        def execute(engine: Engine) -> typing.List[typing.Dict]:
            result: typing.List[typing.Dict] = list()
            with Session(engine) as orm_session:
                for user in self._select_users(orm_session, **kwargs):
                    user_access = list()
                    for access in orm_session.query(_Access).filter_by(user=user.id):
                        user_access.append({
                            'id': access.id,
                            'station': access.station,
                            'mode': access.mode,
                            'write': access.write,
                        })

                    has_password = orm_session.query(_AuthPassword).filter_by(user=user.id).one_or_none() is not None
                    oidc_provider = orm_session.query(_AuthOpenIDConnect).filter_by(user=user.id).one_or_none()
                    if oidc_provider is not None:
                        oidc_provider = oidc_provider.provider

                    authentication = None
                    if has_password and oidc_provider:
                        authentication = 'password+' + oidc_provider
                    elif has_password:
                        authentication = 'password'
                    elif oidc_provider:
                        authentication = oidc_provider

                    result.append({
                        'id': user.id,
                        'name': user.name,
                        'email': user.email,
                        'initials': user.initials,
                        'last_seen': user.last_seen,
                        'access': user_access,
                        'authentication': authentication,
                    })

            return result

        return await self.db.execute(execute)

    async def grant_access(self, stations: typing.List[str], modes: typing.List[str], immediate=False,
                           write=True, **kwargs):
        url_root = None
        if not immediate:
            url_root = CONFIGURATION.AUTHENTICATION.REQUEST.URL
            if not url_root.endswith('/auth/confirm'):
                url_root += '/auth/confirm'

        email_templates: typing.List[typing.Tuple[EmailMessage, typing.Dict]] = list()

        def execute(engine: Engine):
            with Session(engine) as orm_session:
                for user in self._select_users(orm_session, **kwargs):
                    if immediate:
                        for station in stations:
                            for mode in modes:
                                if orm_session.query(_Access).filter_by(user=user.id, station=station.lower(),
                                                                        mode=mode, write=write).one_or_none():
                                    _LOGGER.debug(f"Skipping already granted access '{user.name}' {user.email} ({user.id}) - {station.upper()}/{mode}")
                                    continue
                                orm_session.add(_Access(user=user.id, station=station.lower(), mode=mode, write=write))
                                _LOGGER.info(f"Granting access for '{user.name}' {user.email} ({user.id}) - {station.upper()}/{mode}")
                        continue

                    any_added = False
                    challenge_token = token_urlsafe(32)
                    valid_until = datetime.datetime.utcnow() + datetime.timedelta(days=7)
                    for station in stations:
                        for mode in modes:
                            if orm_session.query(_Access).filter_by(user=user.id, station=station.lower(),
                                                                    mode=mode, write=write).one_or_none():
                                _LOGGER.debug(f"Skipping already granted access '{user.name}' {user.email} ({user.id}) - {station.upper()}/{mode}")
                                continue
                            orm_session.query(_AccessChallenge).filter_by(user=user.id, station=station.lower(),
                                                                          mode=mode).delete()
                            orm_session.add(_AccessChallenge(user=user.id, station=station.lower(), mode=mode,
                                                             write=write, token=challenge_token,
                                                             valid_until=valid_until))
                            any_added = True
                            _LOGGER.info(f"Requesting access confirmation for '{user.name}' {user.email} ({user.id}) - {station.upper()}/{mode}")
                    orm_session.commit()

                    if not any_added:
                        continue

                    template_context = {
                        'stations': stations,
                        'user': user,
                        'confirm_url': f'{url_root}?token={challenge_token}',
                    }

                    message = EmailMessage()
                    message['Subject'] = f"{','.join(stations).upper()} - Access Confirmation"
                    message['To'] = user.email
                    addrs = ', '.join(CONFIGURATION.get('AUTHENTICATION.REQUEST.EMAIL', []))
                    if len(addrs) > 0:
                        message['CC'] = addrs
                        message['Reply-To'] = addrs
                    email_templates.append((message, template_context))

                orm_session.commit()

        await self.db.execute(execute)

        email_futures: typing.List[Future] = list()
        for message in email_templates:
            message[0].set_content(await package_template(
                'access', 'request_challenge_email.txt').render_async(message[1]))
            message[0].add_alternative(await package_template(
                'access', 'request_challenge_email.html').render_async(message[1]), subtype='html')
            email_futures.append(send_email(message[0], CONFIGURATION.get('EMAIL')))

        if len(email_futures) > 0:
            await asyncio.wait([asyncio.wrap_future(f) for f in email_futures])

    async def revoke_access(self, **kwargs):
        def execute(engine: Engine):
            with Session(engine) as orm_session:
                for user in self._select_users(orm_session, **kwargs):
                    revoke = orm_session.query(_Access).filter_by(user=user.id)
                    if kwargs.get('station'):
                        revoke = revoke.filter_by(station=kwargs['station'].lower())
                    if kwargs.get('mode'):
                        revoke = self._mode_filter(revoke, kwargs['mode'])
                    revoke.delete(synchronize_session='fetch')
                    _LOGGER.info(f"Revoked access for '{user.name}' {user.email} ({user.id})")
                orm_session.commit()

        await self.db.execute(execute)

    async def logout_user(self, **kwargs):
        def execute(engine: Engine):
            with Session(engine) as orm_session:
                for user in self._select_users(orm_session, **kwargs):
                    orm_session.query(_Session).filter_by(user=user.id).delete()
                    _LOGGER.debug(f"Cleared sessions for '{user.name}' {user.email} ({user.id})")
                orm_session.commit()

        await self.db.execute(execute)

    async def delete_user(self, **kwargs):
        def execute(engine: Engine):
            with Session(engine) as orm_session:
                self._select_users(orm_session, **kwargs).delete(synchronize_session='fetch')
                orm_session.commit()

        await self.db.execute(execute)

    async def add_user(self, email: str, password: typing.Optional[str], name: typing.Optional[str] = None,
                       initials: typing.Optional[str] = None):
        def execute(engine: Engine):
            with Session(engine) as orm_session:
                user = _User(email=email.lower(), name=name)
                if initials is not None:
                    user.initials = initials
                elif user.name is not None:
                    user.initials = name_to_initials(user.name)
                orm_session.add(user)
                orm_session.flush()

                pbkdf2 = 'x'
                if password is not None:
                    pbkdf2 = pbkdf2_sha256.hash(password)
                auth = _AuthPassword(user=user.id, pbkdf2=pbkdf2)
                orm_session.add(auth)
                orm_session.commit()

                _LOGGER.info(f"Created password login for '{name}' {email} ({user.id})")

        await self.db.execute(execute)

    async def modify_user(self, set_email: typing.Optional[str] = None, set_name: typing.Optional[str] = None,
                          set_initials: typing.Optional[str] = None, set_password: typing.Optional[str] = None,
                          set_last_seen: typing.Optional[datetime.datetime] = None,
                          **kwargs):
        def execute(engine: Engine):
            with Session(engine) as orm_session:
                for user in self._select_users(orm_session, **kwargs):
                    _LOGGER.debug(f"Modifying user '{user.name}' {user.email} ({user.id})")

                    if set_email:
                        user.email = set_email.lower()
                    if set_name:
                        user.name = set_name
                    if set_initials:
                        user.initials = set_initials
                    if set_last_seen:
                        user.last_seen = set_last_seen
                    if set_password:
                        auth = orm_session.query(_AuthPassword).filter_by(user=user.id).one_or_none()
                        if auth:
                            auth.pbkdf2 = pbkdf2_sha256.hash(set_password)
                        else:
                            auth = _AuthPassword(user=user.id, pbkdf2=pbkdf2_sha256.hash(set_password))
                            orm_session.add(auth)
                orm_session.commit()

        await self.db.execute(execute)


class AccessUser(BaseAccessUser):
    def __init__(self, controller: AccessController, user: _User):
        self.controller = controller
        self.auth_user = user
        self._access: Future[typing.List[_Access]] = controller.db.future(self._load_access)

    def _load_access(self, engine: Engine) -> typing.List[_Access]:
        with Session(engine) as orm_session:
            return orm_session.query(_Access).filter_by(user=self.auth_user.id).all()

    @property
    def is_authenticated(self) -> bool:
        return True

    @property
    def can_request_access(self) -> bool:
        return True

    @property
    def display_name(self) -> str:
        if self.auth_user.name is not None and len(self.auth_user.name) != 0:
            return self.auth_user.name
        if self.auth_user.initials is not None and len(self.auth_user.initials) != 0:
            return self.auth_user.initials
        return self.auth_user.email

    @property
    def initials(self) -> str:
        return self.auth_user.initials or ''

    @property
    def display_id(self) -> str:
        return str(self.auth_user.id)

    @property
    def visible_stations(self) -> typing.List[str]:
        result: typing.Set[str] = set()
        for access in self._access.result():
            if access.station == '*':
                return sorted(DISPLAY_STATIONS)
            if str(access.station) not in DISPLAY_STATIONS:
                continue
            result.add(str(access.station))
        return sorted(result)

    def allow_station(self, station: str) -> bool:
        for access in self._access.result():
            if access.station == '*':
                return True
            if str(access.station) == station:
                return True
        return False

    def allow_mode(self, station: str, mode: str, write=False) -> bool:
        for access in self._access.result():
            if access.station != '*' and str(access.station) != station:
                continue
            if not self.matches_mode(access.mode, mode):
                continue
            if not write:
                return True
            if access.write:
                return True
            continue
        return False
