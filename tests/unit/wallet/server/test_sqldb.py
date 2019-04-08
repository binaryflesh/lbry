import unittest
from torba.client.constants import COIN, NULL_HASH32

from lbrynet.schema.claim import Claim
from lbrynet.wallet.server.db import SQLDB
from lbrynet.wallet.transaction import Transaction, Input, Output


def get_output(amount=COIN, pubkey_hash=NULL_HASH32):
    return Transaction() \
        .add_outputs([Output.pay_pubkey_hash(amount, pubkey_hash)]) \
        .outputs[0]


def get_input():
    return Input.spend(get_output())


def get_tx():
    return Transaction().add_inputs([get_input()])


class OldWalletServerTransaction:
    def __init__(self, tx):
        self.tx = tx

    def serialize(self):
        return self.tx.raw


class TestSQLDB(unittest.TestCase):

    def _make_tx(self, output):
        tx = get_tx().add_outputs([output])
        return OldWalletServerTransaction(tx), tx.hash

    def get_channel(self, title, amount, name='@foo'):
        claim = Claim()
        claim.channel.title = title
        channel = Output.pay_claim_name_pubkey_hash(amount, name, claim, b'abc')
        channel.generate_channel_private_key()
        return self._make_tx(channel)

    def get_stream(self, title, amount, name='foo'):
        claim = Claim()
        claim.stream.title = title
        return self._make_tx(Output.pay_claim_name_pubkey_hash(amount, name, claim, b'abc'))

    def get_stream_update(self, tx, amount):
        claim = Transaction(tx[0].serialize()).outputs[0]
        return self._make_tx(
            Output.pay_update_claim_pubkey_hash(
                amount, claim.claim_name, claim.claim_id, claim.claim, b'abc'
            )
        )

    def get_support(self, tx, amount):
        claim = Transaction(tx[0].serialize()).outputs[0]
        return self._make_tx(
            Output.pay_support_pubkey_hash(
                amount, claim.claim_name, claim.claim_id, b'abc'
             )
        )

    def debug(self):
        print('-'*20)
        print('claims:')
        for claim in self.sql.execute("select raw, claim.* from claim join tx using (tx_hash)"):
            txo = Transaction(claim['raw']).outputs[0]
            print([
                txo.claim_id, txo.claim.stream.title, claim['activation_height'],
                claim['height'], claim['effective_amount']
            ])
        print('claimtrie:')
        for claim in self.sql.execute("select * from claimtrie"):
            print(claim)

    def advance(self, height, txs):
        self.sql.advance_txs(height, txs)
        self.debug()

    def state(self, controlling=None, active=None, accepted=None):
        self.assertEqual(controlling, controlling)

    def setUp(self):
        self.sql = SQLDB(':memory:')
        self.sql.open()

    def test_example_from_spec(self):
        # https://spec.lbry.com/#claim-activation-example
        advance, state = self.advance, self.state
        stream = self.get_stream('Claim A', 10*COIN)
        advance(13, [stream])
        state(
            controlling=('Claim A', 10*COIN, 13),
            active=[],
            accepted=[]
        )
        advance(1001, [self.get_stream('Claim B', 20*COIN)])
        state(
            controlling=('Claim A', 10*COIN, 13),
            active=[],
            accepted=[('Claim B', 20*COIN, 1031)]
        )
        advance(1010, [self.get_support(stream, 14*COIN)])
        state(
            controlling=('Claim A', 24*COIN, 13),
            active=[],
            accepted=[('Claim B', 20*COIN, 1031)]
        )
        advance(1020, [self.get_stream('Claim C', 50*COIN)])
        state(
            controlling=('Claim A', 24*COIN, 13),
            active=[],
            accepted=[
                ('Claim B', 20*COIN, 1031),
                ('Claim C', 50*COIN, 1051)]
        )
        advance(1031, [])
        state(
            controlling=('Claim A', 24*COIN, 13),
            active=[('Claim B', 20*COIN, 1031)],
            accepted=[('Claim C', 50*COIN, 1051)]
        )
        advance(1040, [self.get_stream('Claim D', 300*COIN)])
        state(
            controlling=('Claim A', 24*COIN, 13),
            active=[('Claim B', 20*COIN, 1031)],
            accepted=[
                ('Claim C', 50*COIN, 1051),
                ('Claim D', 300*COIN, 1051)]
        )
        advance(1051, [])
        state(
            controlling=('Claim D', 300*COIN, 1051),
            active=[
                ('Claim A', 24*COIN, 1051),
                ('Claim B', 20*COIN, 1051),
                ('Claim C', 50*COIN, 1051)],
            accepted=[]
        )
        # more
        advance(1052, [self.get_stream_update(stream, 290*COIN)])
        state(
            controlling=('Claim D', 300*COIN, 1052),
            active=[
                ('Claim A', 24*COIN, 1052),
                ('Claim B', 20*COIN, 1052),
                ('Claim C', 50*COIN, 1052)],
            accepted=[]
        )
