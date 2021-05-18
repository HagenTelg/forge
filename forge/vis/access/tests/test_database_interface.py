import pytest
import asyncio
import datetime
from forge.vis.access.database import ControlInterface


@pytest.fixture
def interface():
    return ControlInterface("sqlite+pysqlite:///:memory:")


@pytest.mark.asyncio
async def test_basic(interface):
    users = await interface.list_users()
    assert users == []

    await interface.add_user('test@example.com', None, name='Test Name', initials='TST')

    users = await interface.list_users()
    assert users == [
        {
            'id': users[0]['id'],
            'email': 'test@example.com',
            'name': 'Test Name',
            'initials': 'TST',
            'last_seen': None,
            'access': [],
        }
    ]

    await interface.modify_user(email='test@example.com', set_name='New Name')
    await interface.grant_access(['test'], ['test-test'], immediate=True, email='test@example.com')
    users = await interface.list_users()
    assert users == [
        {
            'id': users[0]['id'],
            'email': 'test@example.com',
            'name': 'New Name',
            'initials': 'TST',
            'last_seen': None,
            'access': [
                {
                    'id': users[0]['access'][0]['id'],
                    'station': 'test',
                    'mode': 'test-test',
                    'write': True,
                }
            ],
        }
    ]

    await interface.logout_user(email='test@example.com')
    await interface.delete_user(email='test@example.com')
    users = await interface.list_users()
    assert users == []


@pytest.mark.asyncio
async def test_grant_revoke(interface):
    await interface.add_user('test@example.com', None, name='Test Name', initials='TST')

    await interface.grant_access(['test'], ['test-test'], immediate=True, email='test@example.com')
    users = await interface.list_users()
    assert users == [
        {
            'id': users[0]['id'],
            'email': 'test@example.com',
            'name': 'Test Name',
            'initials': 'TST',
            'last_seen': None,
            'access': [
                {
                    'id': users[0]['access'][0]['id'],
                    'station': 'test',
                    'mode': 'test-test',
                    'write': True,
                }
            ],
        },
    ]

    await interface.revoke_access(email='test@example.com')
    assert (await interface.list_users())[0]['access'] == []

    await interface.grant_access(['test'], ['test-test'], immediate=True, email='test@example.com')
    assert (await interface.list_users()) == users
    await interface.revoke_access(email='test@example.com', station='NONE')
    assert (await interface.list_users()) == users
    await interface.revoke_access(email='test@example.com', station='test')
    assert (await interface.list_users())[0]['access'] == []

    await interface.grant_access(['test'], ['test-test'], immediate=True, email='test@example.com')
    assert (await interface.list_users()) == users
    await interface.revoke_access(mode='NONE')
    assert (await interface.list_users()) == users
    await interface.revoke_access(mode='test-')
    assert (await interface.list_users())[0]['access'] == []


@pytest.mark.asyncio
async def test_selections(interface):
    await interface.add_user('test1@example.com', None, name='Test Name', initials='TS1')
    await interface.grant_access(['test'], ['test-test'], immediate=True, email='test1@example.com')
    await interface.add_user('test@example2.com', None, name='Second Name', initials='TS2')
    await interface.modify_user(email='test@example2.com', set_last_seen=datetime.datetime(2020, 1, 1))

    all_users = await interface.list_users()
    all_users.sort(key=lambda x: x['initials'])
    assert all_users == [
        {
            'id': all_users[0]['id'],
            'email': 'test1@example.com',
            'name': 'Test Name',
            'initials': 'TS1',
            'last_seen': None,
            'access': [
                {
                    'id': all_users[0]['access'][0]['id'],
                    'station': 'test',
                    'mode': 'test-test',
                    'write': True,
                }
            ],
        },
        {
            'id': all_users[1]['id'],
            'email': 'test@example2.com',
            'name': 'Second Name',
            'initials': 'TS2',
            'last_seen': datetime.datetime(2020, 1, 1),
            'access': [],
        },
    ]

    users = await interface.list_users(user=all_users[0]['id'])
    assert users == [all_users[0]]
    users = await interface.list_users(user=all_users[1]['id'])
    assert users == [all_users[1]]

    users = await interface.list_users(email='test')
    assert users == all_users
    users = await interface.list_users(email='test1')
    assert users == [all_users[0]]
    users = await interface.list_users(email='*example.com')
    assert users == [all_users[0]]

    users = await interface.list_users(name='Name')
    assert users == all_users
    users = await interface.list_users(name='Second')
    assert users == [all_users[1]]

    users = await interface.list_users(initials='TS')
    assert users == all_users
    users = await interface.list_users(initials='TS1')
    assert users == [all_users[0]]

    users = await interface.list_users(before=datetime.datetime(2019, 1, 1))
    assert users == []
    users = await interface.list_users(before=datetime.datetime(2020, 2, 1))
    assert users == [all_users[1]]

    users = await interface.list_users(after=datetime.datetime(2020, 2, 1))
    assert users == []
    users = await interface.list_users(after=datetime.datetime(2019, 1, 1))
    assert users == [all_users[1]]

    users = await interface.list_users(never=True)
    assert users == [all_users[0]]

    users = await interface.list_users(station='NONE')
    assert users == []
    users = await interface.list_users(station='test')
    assert users == [all_users[0]]

    users = await interface.list_users(mode='NONE')
    assert users == []
    users = await interface.list_users(mode='test-test')
    assert users == [all_users[0]]
    users = await interface.list_users(mode='*')
    assert users == []
    users = await interface.list_users(mode='test-%')
    assert users == [all_users[0]]
