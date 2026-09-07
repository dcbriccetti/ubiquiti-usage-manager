import hashlib
from contextlib import closing
import importlib.util
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from types import ModuleType
from typing import TypeAlias
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
import database as db
import voucher_repository

ReportKey: TypeAlias = tuple[str, str, str, str, str]

spec = importlib.util.spec_from_file_location('rollup_validation', ROOT / 'deploy/scripts/validate-meter-rollups.py')
if spec is None or spec.loader is None:
    raise RuntimeError('Could not load the rollup validation module')
validation = importlib.util.module_from_spec(spec)
spec.loader.exec_module(validation)
assert isinstance(validation, ModuleType)


class OfflineRollupTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp: tempfile.TemporaryDirectory[str] = tempfile.TemporaryDirectory()
        self.root: Path = Path(self.temp.name)
        self.source: Path = self.root / 'source.db'
        self.engine: Engine = create_engine(f'sqlite:///{self.source}')
        db.Base.metadata.create_all(self.engine)
        self.sessions: sessionmaker[Session] = sessionmaker(bind=self.engine)
        self.start: datetime = datetime(2026, 8, 31, 23, 55)
        with self.sessions() as session:
            session.add(db.PlusVoucher(id=1, batch_id='test', user_id=10, password='test', allocation_gb=10, generated_at=self.start-timedelta(days=1)))
            # Repeated observations, shared IP reassignment, multiple devices,
            # equal-time observation ordering, and a future observation in tolerance.
            for at,ip,mac,user in [
                (self.start-timedelta(hours=1),'a','mac-a','10'),
                (self.start,'a','mac-a','10'),
                (self.start,'b','mac-b','10'),
                (self.start+timedelta(minutes=10),'c','mac-c','10'),
                (self.start+timedelta(hours=1),'a','mac-a','10'),
                (self.start+timedelta(hours=1),'a','mac-other','20'),
                (self.start+timedelta(days=7),'a','mac-other','20'),
                (self.start+timedelta(days=7),'b','mac-b','10'),
            ]:
                session.add(db.ClientIpIdentity(observed_at=at, ip_address=ip, mac=mac, name=mac, user_id=user, vlan='Plus'))
            for n,(offset,ip,direction) in enumerate([
                (0,'a','upload'),(4,'c','download'),(5,'b','download'),
                (65,'a','download'),(70,'unknown','download'),
                (7*1440+10,'b','upload'),(7*1440+15,'a','download')]):
                at=self.start+timedelta(minutes=offset)
                session.add(db.WanFlowUsage(source_file=f'capture-{n}',started_at=at,
                    ended_at=at+timedelta(minutes=15), duration_seconds=900,proto='TCP',
                    src_ip=ip,dst_ip='remote',src_port=1,dst_port=443,packets=1,
                    bytes=(n+1)*1_000_000,direction=direction,client_ip=ip))
            session.commit()

    def tearDown(self) -> None:
        self.engine.dispose()
        self.temp.cleanup()

    def test_rebuild_preserves_source_and_matches_real_voucher_function(self) -> None:
        before=hashlib.sha256(self.source.read_bytes()).hexdigest()
        result=validation.build(self.source,self.root/'out',progress=False)
        self.assertTrue(result['passed'],result)
        self.assertEqual(before,hashlib.sha256(self.source.read_bytes()).hexdigest())
        with patch.object(db,'SessionLocal',self.sessions):
            voucher=db.get_plus_voucher(1)
            self.assertIsNotNone(voucher)
            assert voucher is not None
            first,mb=voucher_repository._get_plus_voucher_wan_usage_summaries(
                [voucher],period_end=self.start+timedelta(days=8))[voucher.id]
        self.assertIsNotNone(first)
        assert first is not None
        self.assertEqual(result['vouchers'][0]['prototype_bytes'],round(mb*1_000_000))
        self.assertEqual(result['vouchers'][0]['prototype_activated_at'],validation.stamp(first))
        with self.assertRaises(FileExistsError):
            validation.build(self.source,self.root/'out',progress=False)

    def test_streamed_reference_matches_real_report_function(self) -> None:
        with closing(sqlite3.connect(self.source)) as source:
            end=self.start+timedelta(days=8)
            for start in [self.start,datetime(2026,9,1),end-timedelta(days=7)]:
                history=validation.identities(source,start,end)
                expected: dict[ReportKey, list[int]] = {}
                for at,ip,direction,size in validation.flows(source,start,end):
                    key=validation.report_key(ip,validation.resolve(history,ip,at))
                    value=expected.setdefault(key,[0,0,0])
                    value[0 if direction=='upload' else 1]+=size
                    value[2]+=1
                with patch.object(db,'SessionLocal',self.sessions):
                    rows=db.get_wan_usage_by_identity(start,end)
                actual={validation.report_key(r.client_ip,(r.mac,r.name,r.user_id,r.vlan)):
                        [r.upload_bytes,r.download_bytes,r.flow_count] for r in rows}
                self.assertEqual(expected,actual)

    def test_window_dependent_identity_is_reported_as_failure(self) -> None:
        with self.sessions() as session:
            session.add(db.ClientIpIdentity(observed_at=self.start-timedelta(days=10),
                ip_address='unknown',mac='old-mac',name='old',user_id='30',vlan='Basic'))
            session.commit()
        result=validation.build(self.source,self.root/'out',progress=False)
        self.assertTrue(result['conservation']['passed'])
        self.assertFalse(result['passed'])
        self.assertTrue(any(not row['passed'] for row in result['report_comparisons']))

    def test_duplicate_active_user_is_rejected(self) -> None:
        with self.sessions() as session:
            session.add(db.PlusVoucher(batch_id='test',user_id=10,password='test',allocation_gb=10,generated_at=self.start))
            session.commit()
        with self.assertRaisesRegex(ValueError,'Multiple active vouchers'):
            validation.build(self.source,self.root/'out',progress=False)


if __name__ == '__main__':
    unittest.main()
