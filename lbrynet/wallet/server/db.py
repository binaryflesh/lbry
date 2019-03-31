import msgpack
import struct
import sqlite3
import time
from binascii import hexlify
from typing import Union, Tuple, List

from torba.server.db import DB
from torba.server.hash import hash_to_hex_str
from torba.client.basedatabase import query

from lbrynet.wallet.server.model import ClaimInfo


class SQLDB:

    TRENDING_BLOCKS = 300  # number of blocks over which to calculate trending

    PRAGMAS = """
        pragma journal_mode=WAL;
    """

    CREATE_CLAIM_TABLE = """
        create table if not exists claim (
            txid bytes not null,
            nout integer not null,
            block integer not null,
            amount integer not null,
            effective_amount integer not null default 0,
            trending_amount integer not null default 0,
            claim_id bytes not null,
            claim_name text not null,
            channel_id bytes
        );
        create index if not exists claim_claim_id_idx on claim (claim_id);
        create index if not exists claim_claim_name_idx on claim (claim_name);
        create index if not exists claim_channel_id_idx on claim (channel_id);
    """

    CREATE_SUPPORT_TABLE = """
        create table if not exists support (
            txid bytes not null,
            nout integer not null,
            block integer not null,
            amount integer not null,
            claim_id bytes not null
        );
        create index if not exists support_claim_id_idx on support (claim_id);
    """

    CREATE_CLAIMTRIE_TABLE = """
        create table if not exists claimtrie (
            claim_name text not null,
            claim_id bytes not null,
            block integer not null
        );
        create index if not exists claimtrie_claim_id_idx on claimtrie (claim_id);
        create index if not exists claimtrie_claim_name_idx on claimtrie (claim_name);
    """

    CREATE_TAG_TABLE = """
        create table if not exists tag (
            tag text,
            claim_id bytes,
            block integer
        );
        create index if not exists tag_tag_idx on tag (tag);
        create index if not exists tag_claim_id_idx on tag (claim_id);
    """

    CREATE_LOCATION_TABLE = """
        create table if not exists location (
            country integer,
            state text,
            claim_id bytes,
            block integer
        );
        create index if not exists location_country_idx on location (country);
        create index if not exists location_state_idx on location (state);
        create index if not exists location_claim_id_idx on location (claim_id);
    """

    CREATE_LANGUAGE_TABLE = """
        create table if not exists lang (
            lang integer,
            claim_id bytes,
            block integer
        );
        create index if not exists lang_lang_idx on lang (lang);
        create index if not exists lang_claim_id_idx on lang (claim_id);
    """

    CREATE_TABLES_QUERY = (
        PRAGMAS +
        CREATE_CLAIM_TABLE +
        CREATE_SUPPORT_TABLE +
        CREATE_CLAIMTRIE_TABLE +
        CREATE_TAG_TABLE +
        CREATE_LOCATION_TABLE +
        CREATE_LANGUAGE_TABLE
    )

    def __init__(self, path):
        self._db_path = path
        self.db = None

    def open(self):
        self.db = sqlite3.connect(self._db_path, check_same_thread=False)
        self.db.executescript(self.CREATE_TABLES_QUERY)

    def close(self):
        self.db.close()

    @staticmethod
    def _insert_sql(table: str, data: dict, ignore_duplicate: bool = False) -> Tuple[str, List]:
        columns, values = [], []
        for column, value in data.items():
            columns.append(column)
            values.append(value)
        or_ignore = ""
        if ignore_duplicate:
            or_ignore = " OR IGNORE"
        sql = "INSERT{} INTO {} ({}) VALUES ({})".format(
            or_ignore, table, ', '.join(columns), ', '.join(['?'] * len(values))
        )
        return sql, values

    @staticmethod
    def _update_sql(table: str, data: dict, where: str,
                    constraints: Union[list, tuple]) -> Tuple[str, list]:
        columns, values = [], []
        for column, value in data.items():
            columns.append("{} = ?".format(column))
            values.append(value)
        values.extend(constraints)
        sql = "UPDATE {} SET {} WHERE {}".format(
            table, ', '.join(columns), where
        )
        return sql, values

    @staticmethod
    def claim_to_row(txo, channel_hash):
        tx = txo.tx_ref.tx
        return {
            'txid': sqlite3.Binary(tx.hash),
            'nout': txo.position,
            'block': tx.height,
            'amount': txo.amount,
            'claim_id': sqlite3.Binary(txo.claim_hash),
            'claim_name': txo.claim_name,
            'channel_id': sqlite3.Binary(channel_hash) if channel_hash is not None else None
        }

    def insert_claim(self, txo):
        height = txo.tx_ref.tx.height
        self.db.execute(*self._insert_sql(
            "claim", self.claim_to_row(txo, None)
        ))
        claim_hash = sqlite3.Binary(txo.claim_hash)
        if txo.claim.is_channel:
            claim = txo.claim.channel
        else:
            claim = txo.claim.stream
        for tag in claim.tags:
            self.db.execute(*self._insert_sql(
                "tag", {
                    'tag': tag,
                    'claim_id': claim_hash,
                    'block': height
                }
            ))
        for location in claim.locations:
            self.db.execute(*self._insert_sql(
                "location", {
                    'country': location.message.country,
                    'state': location.message.state,
                    'claim_id': claim_hash,
                    'block': height
                }
            ))
        for lang in claim.languages:
            self.db.execute(*self._insert_sql(
                "lang", {
                    'lang': lang.message.language,
                    'claim_id': claim_hash,
                    'block': height
                }
            ))

    def update_claim(self, txo):
        self.delete_claim(txo.claim_hash, permanent=False)
        self.insert_claim(txo)

    def delete_claim(self, claim_hash, permanent=True):
        claim_hash = sqlite3.Binary(claim_hash)
        tables = ['claim', 'tag', 'lang', 'location']
        if permanent:
            self.db.execute(f"UPDATE claimtrie SET claim_id = NULL WHERE claim_id = ?", (claim_hash,))
            tables.append('support')
        for table in tables:
            self.db.execute(f"DELETE FROM {table} WHERE claim_id = ?", (claim_hash,))

    def insert_support(self, txo):
        tx = txo.tx_ref.tx
        self.db.execute(*self._insert_sql(
            "support", {
                'txid': sqlite3.Binary(tx.hash),
                'nout': txo.position,
                'block': tx.height,
                'amount': txo.amount,
                'claim_id': sqlite3.Binary(txo.claim_hash)
            }
        ))

    def maybe_delete_supports(self, txis):
        for txi in txis:
            txid, nout = sqlite3.Binary(txi.txo_ref.tx_ref.hash), txi.txo_ref.position
            self.db.execute(f"DELETE FROM support WHERE txid = ? AND nout = ?", (txid, nout))

    def update_claimtrie(self, height):
        cur = self.db.cursor()
        cur.execute(f"""
            UPDATE claim SET
              effective_amount = COALESCE(
                (SELECT SUM(amount) FROM support WHERE support.claim_id=claim.claim_id), 0
              ) + claim.amount,
              trending_amount = COALESCE(
                (SELECT SUM(amount) FROM support WHERE
                   support.claim_id=claim.claim_id AND support.block > {height-self.TRENDING_BLOCKS}), 0
              )
        """)
        cur.execute("""
            SELECT DISTINCT claim.claim_name, trie.claim_id
            FROM claim LEFT JOIN claimtrie AS trie USING (claim_name)
        """)
        for claim_name, winning_id in cur.fetchall():
            cur.execute("""
                SELECT claim_id FROM claim
                WHERE claim_name = ?
                ORDER BY effective_amount DESC
                LIMIT 1
            """, (claim_name,))
            new_winner = cur.fetchone()
            if winning_id is None:
                self.db.execute(*self._insert_sql(
                    "claimtrie", {
                        'claim_name': claim_name,
                        'claim_id': sqlite3.Binary(new_winner[0]),
                        'block': height
                    }
                ))
            elif new_winner[0] != winning_id:
                self.db.execute(*self._update_sql(
                    "claimtrie", {
                        'claim_id': sqlite3.Binary(new_winner[0]),
                        'block': height
                    }, 'claim_name = ?', (claim_name,)
                ))

    def select_claims(self, cols, **constraints):
        cur = self.db.cursor()
        cur.execute(*query(
            "SELECT {} FROM claim LEFT JOIN claimtrie USING (claim_id)".format(cols), **constraints
        ))
        return cur.fetchall()

    def get_claims(self, **constraints):
        if 'order_by' not in constraints:
            constraints['order_by'] = ["claim.block DESC"]
        if 'is_winning' in constraints:
            if constraints['is_winning']:
                constraints['claimtrie.claim_id__is_not_null'] = ''
            else:
                constraints['claimtrie.claim_id__is_null'] = ''
            del constraints['is_winning']
        return [{
            'claim_name': r[0],
            'claim_id': hexlify(r[1][::-1]).decode(),
            'txid': hexlify(r[2][::-1]).decode(),
            'nout': r[3], 'amount': r[4], 'effective_amount': r[5], 'trending_amount': r[6],
            'is_winning': bool(r[7])
            } for r in self.select_claims(
            "claim.claim_name, claim.claim_id, txid, nout, "
            "amount, effective_amount, trending_amount, "
            "claimtrie.claim_id", **constraints
        )]


class LBRYDB(DB):

    def __init__(self, *args, **kwargs):
        self.sqldb = SQLDB(':memory:')
        self.claim_cache = {}
        self.claims_signed_by_cert_cache = {}
        self.outpoint_to_claim_id_cache = {}
        self.claims_db = self.signatures_db = self.outpoint_to_claim_id_db = self.claim_undo_db = None
        # stores deletes not yet flushed to disk
        self.pending_abandons = {}
        super().__init__(*args, **kwargs)

    def close(self):
        self.batched_flush_claims()
        self.claims_db.close()
        self.signatures_db.close()
        self.outpoint_to_claim_id_db.close()
        self.claim_undo_db.close()
        self.utxo_db.close()
        self.sqldb.close()
        super().close()

    async def _open_dbs(self, for_sync, compacting):
        self.sqldb.open()
        await super()._open_dbs(for_sync=for_sync, compacting=compacting)
        def log_reason(message, is_for_sync):
            reason = 'sync' if is_for_sync else 'serving'
            self.logger.info('{} for {}'.format(message, reason))

        if self.claims_db:
            if self.claims_db.for_sync == for_sync:
                return
            log_reason('closing claim DBs to re-open', for_sync)
            self.claims_db.close()
            self.signatures_db.close()
            self.outpoint_to_claim_id_db.close()
            self.claim_undo_db.close()
        self.claims_db = self.db_class('claims', for_sync)
        self.signatures_db = self.db_class('signatures', for_sync)
        self.outpoint_to_claim_id_db = self.db_class('outpoint_claim_id', for_sync)
        self.claim_undo_db = self.db_class('claim_undo', for_sync)
        log_reason('opened claim DBs', self.claims_db.for_sync)

    def flush_dbs(self, flush_data, flush_utxos, estimate_txs_remaining):
        # flush claims together with utxos as they are parsed together
        self.batched_flush_claims()
        return super().flush_dbs(flush_data, flush_utxos, estimate_txs_remaining)

    def batched_flush_claims(self):
        with self.claims_db.write_batch() as claims_batch:
            with self.signatures_db.write_batch() as signed_claims_batch:
                with self.outpoint_to_claim_id_db.write_batch() as outpoint_batch:
                    self.flush_claims(claims_batch, signed_claims_batch, outpoint_batch)

    def flush_claims(self, batch, signed_claims_batch, outpoint_batch):
        flush_start = time.time()
        write_claim, write_cert = batch.put, signed_claims_batch.put
        write_outpoint = outpoint_batch.put
        delete_claim, delete_outpoint = batch.delete, outpoint_batch.delete
        delete_cert = signed_claims_batch.delete
        for claim_id, outpoints in self.pending_abandons.items():
            claim = self.get_claim_info(claim_id)
            if claim.cert_id:
                self.remove_claim_from_certificate_claims(claim.cert_id, claim_id)
            self.remove_certificate(claim_id)
            self.claim_cache[claim_id] = None
            for txid, tx_index in outpoints:
                self.put_claim_id_for_outpoint(txid, tx_index, None)
        for key, claim in self.claim_cache.items():
            if claim:
                write_claim(key, claim)
            else:
                delete_claim(key)
        for cert_id, claims in self.claims_signed_by_cert_cache.items():
            if not claims:
                delete_cert(cert_id)
            else:
                write_cert(cert_id, msgpack.dumps(claims))
        for key, claim_id in self.outpoint_to_claim_id_cache.items():
            if claim_id:
                write_outpoint(key, claim_id)
            else:
                delete_outpoint(key)
        self.logger.info('flushed at height {:,d} with {:,d} claims, {:,d} outpoints '
                         'and {:,d} certificates added while {:,d} were abandoned in {:.1f}s, committing...'
                         .format(self.db_height,
                                 len(self.claim_cache), len(self.outpoint_to_claim_id_cache),
                                 len(self.claims_signed_by_cert_cache), len(self.pending_abandons),
                                 time.time() - flush_start))
        self.claim_cache = {}
        self.claims_signed_by_cert_cache = {}
        self.outpoint_to_claim_id_cache = {}
        self.pending_abandons = {}

    def assert_flushed(self, flush_data):
        super().assert_flushed(flush_data)
        assert not self.claim_cache
        assert not self.claims_signed_by_cert_cache
        assert not self.outpoint_to_claim_id_cache
        assert not self.pending_abandons

    def abandon_spent(self, tx_hash, tx_idx):
        claim_id = self.get_claim_id_from_outpoint(tx_hash, tx_idx)
        if claim_id:
            self.logger.info("[!] Abandon: {}".format(hash_to_hex_str(claim_id)))
            self.pending_abandons.setdefault(claim_id, []).append((tx_hash, tx_idx,))
            return claim_id

    def put_claim_id_for_outpoint(self, tx_hash, tx_idx, claim_id):
        self.logger.info("[+] Adding outpoint: {}:{} for {}.".format(hash_to_hex_str(tx_hash), tx_idx,
                                                                     hash_to_hex_str(claim_id) if claim_id else None))
        self.outpoint_to_claim_id_cache[tx_hash + struct.pack('>I', tx_idx)] = claim_id

    def remove_claim_id_for_outpoint(self, tx_hash, tx_idx):
        self.logger.info("[-] Remove outpoint: {}:{}.".format(hash_to_hex_str(tx_hash), tx_idx))
        self.outpoint_to_claim_id_cache[tx_hash + struct.pack('>I', tx_idx)] = None

    def get_claim_id_from_outpoint(self, tx_hash, tx_idx):
        key = tx_hash + struct.pack('>I', tx_idx)
        return self.outpoint_to_claim_id_cache.get(key) or self.outpoint_to_claim_id_db.get(key)

    def get_signed_claim_ids_by_cert_id(self, cert_id):
        if cert_id in self.claims_signed_by_cert_cache:
            return self.claims_signed_by_cert_cache[cert_id]
        db_claims = self.signatures_db.get(cert_id)
        return msgpack.loads(db_claims, use_list=True) if db_claims else []

    def put_claim_id_signed_by_cert_id(self, cert_id, claim_id):
        msg = "[+] Adding signature: {} - {}".format(hash_to_hex_str(claim_id), hash_to_hex_str(cert_id))
        self.logger.info(msg)
        certs = self.get_signed_claim_ids_by_cert_id(cert_id)
        certs.append(claim_id)
        self.claims_signed_by_cert_cache[cert_id] = certs

    def remove_certificate(self, cert_id):
        msg = "[-] Removing certificate: {}".format(hash_to_hex_str(cert_id))
        self.logger.info(msg)
        self.claims_signed_by_cert_cache[cert_id] = []

    def remove_claim_from_certificate_claims(self, cert_id, claim_id):
        msg = "[-] Removing signature: {} - {}".format(hash_to_hex_str(claim_id), hash_to_hex_str(cert_id))
        self.logger.info(msg)
        certs = self.get_signed_claim_ids_by_cert_id(cert_id)
        if claim_id in certs:
            certs.remove(claim_id)
        self.claims_signed_by_cert_cache[cert_id] = certs

    def get_claim_info(self, claim_id):
        serialized = self.claim_cache.get(claim_id) or self.claims_db.get(claim_id)
        return ClaimInfo.from_serialized(serialized) if serialized else None

    def put_claim_info(self, claim_id, claim_info):
        self.logger.info("[+] Adding claim info for: {}".format(hash_to_hex_str(claim_id)))
        self.claim_cache[claim_id] = claim_info.serialized

    def get_update_input(self, claim_id, inputs):
        claim_info = self.get_claim_info(claim_id)
        if not claim_info:
            return False
        for input in inputs:
            if (input.txo_ref.tx_ref.hash, input.txo_ref.position) == (claim_info.txid, claim_info.nout):
                return input
        return False

    def write_undo(self, pending_undo):
        with self.claim_undo_db.write_batch() as writer:
            for height, undo_info in pending_undo:
                writer.put(struct.pack(">I", height), msgpack.dumps(undo_info))
