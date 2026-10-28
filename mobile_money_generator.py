
from itertools import chain
import numpy as np

START = 1788213600  # 2026-09-01 00:00:00 CAT, UTC+02:00
PERSON_ACCOUNTS = 240_000
ACCOUNTS = 248_000
CUSTOMERS = 228_000
DEVICES = 228_600
BACKGROUND_TRANSFERS = 20_000_000
REGIONS = ('Kigali', 'Northern', 'Southern', 'Eastern', 'Western')
FIELDS = {
    'customers': ('customer_id', 'region'),
    'accounts': ('account_id', 'customer_id', 'role'),
    'devices': ('device_id', 'access_type'),
    'account_devices': ('account_id', 'device_id', 'first_day', 'last_day', 'observations'),
    'transfers': ('transfer_id', 'sender_id', 'receiver_id', 'occurred_at', 'amount_rwf'),
}

def owner(account):
    return (account - 1) % 220_000 + 1 if account <= PERSON_ACCOUNTS else account - 20_000

def iter_customers():
    for i in range(1, CUSTOMERS + 1):
        yield dict(customer_id=i, region=REGIONS[((i - 1) // 1500) % 5])

def iter_accounts():
    for i in range(1, ACCOUNTS + 1):
        role = 'person' if i <= PERSON_ACCOUNTS else 'merchant' if i <= 246_000 else 'agent'
        yield dict(account_id=i, customer_id=owner(i), role=role)

def iter_devices():
    for i in range(1, DEVICES + 1):
        yield dict(device_id=i, access_type='public_terminal' if 220_000 < i <= 221_000 else 'handset')

def _members(case):
    # A permutation gives each scenario distinct members, spread across regions.
    return [1 + ((case * 10 + slot) * 7919 + 997) % PERSON_ACCOUNTS for slot in range(4 + case % 5)]

def iter_account_devices():
    for a in range(1, PERSON_ACCOUNTS + 1):
        customer = owner(a)
        # Some households share a phone. Secondary wallets share their owner's phone.
        phone = ((customer - 1) // 5) * 5 + 1 if (customer - 1) % 25 < 5 else customer
        replacement = a % 8 == 0
        yield dict(account_id=a, device_id=phone, first_day=11 if replacement else 0,
                   last_day=29, observations=10 + a % 51)
        if replacement:
            yield dict(account_id=a, device_id=221_001 + (customer * 13) % 7000,
                       first_day=0, last_day=10, observations=2 + a % 9)
        if a % 20 == 0:
            yield dict(account_id=a, device_id=220_001 + (a // 20 * 31) % 200,
                       first_day=a % 7, last_day=22 + a % 8, observations=2 + (a // 20) % 4)
    for case in range(600):
        for a in _members(case):
            yield dict(account_id=a, device_id=228_001 + case,
                       first_day=0, last_day=29, observations=4 + (a % 17))

def _background():
    # The internal fixed state is part of this dataset, not a student parameter.
    rng = np.random.Generator(np.random.PCG64(20260901))
    batch = 100_000
    for offset in range(0, BACKGROUND_TRANSFERS, batch):
        size = min(batch, BACKGROUND_TRANSFERS - offset)
        ids = np.arange(offset + 1, offset + size + 1, dtype=np.int64)
        people = rng.integers(1, PERSON_ACCOUNTS + 1, size, dtype=np.int64)
        choice = rng.integers(0, 100, size)
        contact = rng.integers(1, 9, size, dtype=np.int64)
        # Most P2P transfers repeat a local contact. Some are cross-region remittances.
        local = ((people - 1) // 1500) * 1500 + ((people - 1 + contact * 113) % 1500) + 1
        remote = ((people * 719 + contact * 3571) % PERSON_ACCOUNTS) + 1
        partner = np.where(choice < 48, local, remote)
        partner = np.where(partner == people, people % PERSON_ACCOUNTS + 1, partner)
        sender = people.copy()
        receiver = partner.copy()
        # Unequal popularity produces busy shops and agents, not fixed-degree blocks.
        shops = 240_001 + np.minimum(rng.zipf(1.35, size) - 1, 5999)
        agents = 246_001 + ((people - 1) // 120 + contact) % 2000
        receiver = np.where((choice >= 65) & (choice < 85), shops, receiver)
        receiver = np.where((choice >= 85) & (choice < 93), agents, receiver)
        sender = np.where(choice >= 93, agents, sender)
        receiver = np.where(choice >= 93, people, receiver)
        day = (ids - 1) * 30 // BACKGROUND_TRANSFERS
        second = ((ids - 1) * 30 % BACKGROUND_TRANSFERS) * (16 * 3600) // BACKGROUND_TRANSFERS
        occurred = START + day * 86400 + 6 * 3600 + second
        amount = np.clip(np.rint(rng.lognormal(8.8, 1.05, size) / 100) * 100, 100, 2_000_000).astype(np.int64)
        for row in zip(ids.tolist(), sender.tolist(), receiver.tolist(), occurred.tolist(), amount.tolist()):
            yield dict(zip(FIELDS['transfers'], row))

def _scenario_transfers():
    identifier = BACKGROUND_TRANSFERS
    for case in range(600):
        members = _members(case)
        suspect = case < 300
        circular = case % 2 == 0
        path = members + [members[0]] if circular else members
        base = START + (5 + case % 10) * 86400 + 10 * 3600 + case % 60
        amount = 200_000 + (case % 20) * 50_000 if suspect else 30_000 + case % 9 * 5000
        for step, (a, b) in enumerate(zip(path, path[1:])):
            identifier += 1
            yield dict(transfer_id=identifier, sender_id=a, receiver_id=b,
                       occurred_at=base + step * (40 if suspect else 2 * 86400),
                       amount_rwf=amount * (98 ** step) // (100 ** step) if suspect else amount)

def iter_transfers():
    yield from chain(_background(), _scenario_transfers())

def dataset_info():
    return dict(period='1 to 30 September 2026 CAT', currency='RWF',
                expected_counts=dict(customers=CUSTOMERS, accounts=ACCOUNTS,
                    devices=DEVICES, account_devices=285_600, transfers=20_003_300),
                fields=FIELDS)
