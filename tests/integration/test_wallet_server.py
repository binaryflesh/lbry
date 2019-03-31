from binascii import unhexlify
from integration.testcase import CommandTestCase


class TestClaimtrie(CommandTestCase):

    def get_claim_id(self, tx):
        return tx['outputs'][0]['claim_id']

    def assertWinningClaim(self, tx):
        spv = self.conductor.spv_node.server
        self.assertEqual(
            self.get_claim_id(tx),
            spv.db.sqldb.get_claims(is_winning=True)[0]['claim_id']
        )

    async def test_designed_edge_cases(self):
        tx1 = await self.channel_create('@foo', allow_duplicate_name=True)
        self.assertWinningClaim(tx1)
        tx2 = await self.channel_create('@foo', allow_duplicate_name=True)
        self.assertWinningClaim(tx1)
        tx3 = await self.channel_create('@foo', allow_duplicate_name=True)
        self.assertWinningClaim(tx1)
        await self.support_create(self.get_claim_id(tx3), '0.09')
        self.assertWinningClaim(tx3)
        await self.support_create(self.get_claim_id(tx2), '0.19')
        self.assertWinningClaim(tx2)
        await self.support_create(self.get_claim_id(tx1), '0.19')
        self.assertWinningClaim(tx2)
