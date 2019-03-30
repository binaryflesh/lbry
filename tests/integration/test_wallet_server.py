from binascii import unhexlify
from integration.testcase import CommandTestCase


class TestWalletServer(CommandTestCase):

    async def test_sync(self):
        spv = self.conductor.spv_node.server
        tx1 = await self.channel_create('@foo', allow_duplicate_name=True)
        await self.support_create(tx1['outputs'][0]['claim_id'], '0.09')
        claim1_id = unhexlify(tx1['outputs'][0]['claim_id'])[::-1]
        tx2 = await self.channel_create('@foo', allow_duplicate_name=True)
        claim2_id = unhexlify(tx2['outputs'][0]['claim_id'])[::-1]
        tx3 = await self.channel_create('@foo', allow_duplicate_name=True)
        claim3_id = unhexlify(tx3['outputs'][0]['claim_id'])[::-1]
        txos = spv.db.sqldb.get_claims()
        print('hi')
