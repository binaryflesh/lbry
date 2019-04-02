from typing import Tuple, List, Optional, Dict

from .types.v2.page_pb2 import Page as PageMessage
from lbrynet.wallet.transaction import Transaction


def from_page(data: bytes) -> Tuple[List, int, int]:
    page = PageMessage()
    page.ParseFromString(data)
    txs = {}
    for tx_message in page.txs:
        tx = Transaction(tx_message.tx, height=tx_message.height, position=tx_message.position)
        txs[tx.hash] = tx
    if not page.txos:
        return list(txs.values()), page.offset, page.total
    result = []
    for txo_message in page.txos:
        output = txs[txo_message.tx_hash].outputs[txo_message.nout]
        if txo_message.WhichOneof('meta') == 'claim':
            claim = txo_message.claim
            output.meta = {
                'is_winning': claim.is_winning,
                'effective_amount': claim.effective_amount,
                'trending_amount': claim.trending_amount,
            }
            if claim.HasField('channel'):
                output.channel = txs[claim.channel.tx_hash].outputs[claim.channel.nout]
        result.append(output)
    return result, page.offset, page.total


def to_page(txos: List[Dict], txs: List[Dict], offset: int, total: int) -> bytes:
    page = PageMessage()
    page.total = total
    page.offset = offset
    for tx in txs:
        tx_message = page.txs.add()
        tx_message.tx = tx['tx']
        tx_message.height = tx['block']
        tx_message.position = tx['pos']
    for txo in txos:
        txo_message = page.txos.add()
        txo_message.tx_hash = txo['txid']
        txo_message.nout = txo['nout']
        if 'channel_txid' in txo and txo['channel_txid']:
            txo_message.claim.channel.tx_hash = txo['channel_txid']
            txo_message.claim.channel.nout = txo['channel_nout']
        if 'is_winning' in txo:
            txo_message.claim.is_winning = bool(txo['is_winning'])
        if 'effective_amount' in txo:
            txo_message.claim.effective_amount = txo['effective_amount']
        if 'trending_amount' in txo:
            txo_message.claim.trending_amount = txo['trending_amount']
    return page.SerializeToString()
