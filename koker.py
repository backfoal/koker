import base64
import threading
import time
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import close_old_connections
from django.db.models import Count, Q
from django.utils import timezone
import grpc
from jito_searcher_client.searcher import SearcherInterceptor
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction

from trading.jito_pb import bundle_pb2, packet_pb2, searcher_pb2, searcher_pb2_grpc
from trading.models import BundleOutcome, PendingBundle


class Command(BaseCommand):
    help = "Send pending bundles to the Jito block engine and record outcomes"

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=20)
        parser.add_argument("--wait", type=int, default=30)
        parser.add_argument("--dry-run", action="store_true")

    def handle(self, *args, **options):
        key_path = Path(settings.JITO_KEYPAIR_PATH)
        if not key_path.exists():
            raise CommandError("keypair file not found: %s" % key_path)
        keypair = Keypair.from_json(key_path.read_text())

        url = settings.JITO_BLOCK_ENGINE_URL
        interceptor = SearcherInterceptor(url, keypair)
        channel = grpc.intercept_channel(
            grpc.secure_channel(url, grpc.ssl_channel_credentials()),
            interceptor,
        )
        client = searcher_pb2_grpc.SearcherServiceStub(channel)

        sent_uuids = set()
        listener = threading.Thread(target=self.listen, args=(client, sent_uuids), daemon=True)
        listener.start()

        if options["dry_run"]:
            count = PendingBundle.objects.filter(status="pending").count()
            self.stdout.write(self.style.WARNING("Dry run: %d pending bundles." % count))
            return

        pending = PendingBundle.objects.filter(status="pending").order_by("created_at")[: options["limit"]]
        for item in pending:
            try:
                packets = []
                for raw in item.transactions:
                    tx = VersionedTransaction.from_bytes(base64.b64decode(raw))
                    wire = bytes(tx)
                    packets.append(packet_pb2.Packet(data=wire, meta=packet_pb2.Meta(size=len(wire))))
                bundle = bundle_pb2.Bundle(header=None, packets=packets)
                uuid = client.SendBundle(searcher_pb2.SendBundleRequest(bundle=bundle)).uuid
            except Exception as exc:
                item.status = "failed"
                item.error = str(exc)[:500]
                item.save()
                self.stderr.write("bundle %s failed: %s" % (item.pk, exc))
                continue

            sent_uuids.add(uuid)
            item.status = "sent"
            item.bundle_uuid = uuid
            item.sent_at = timezone.now()
            item.save()
            self.stdout.write(self.style.SUCCESS("Sent %s as %s." % (item.pk, uuid)))

        self.stdout.write(self.style.SUCCESS("Sent %d bundles, waiting %ds for results." % (len(sent_uuids), options["wait"])))
        time.sleep(options["wait"])

        if not sent_uuids:
            self.stdout.write(self.style.WARNING("Nothing was sent this run."))
            return

        landed = BundleOutcome.objects.filter(bundle_uuid__in=sent_uuids).aggregate(
            landed=Count("id", filter=Q(landed=True)),
        )["landed"]
        total = len(sent_uuids)
        rate = 100.0 * landed / total
        self.stdout.write(self.style.SUCCESS("Land rate: %.1f%% (%d of %d)." % (rate, landed, total)))

    def listen(self, client, sent_uuids):
        try:
            stream = client.SubscribeBundleResults(searcher_pb2.SubscribeBundleResultsRequest())
            for res in stream:
                if res.bundle_id not in sent_uuids:
                    continue
                kind = res.WhichOneof("result")
                landed = kind in ("processed", "finalized")
                row = BundleOutcome.objects.filter(bundle_uuid=res.bundle_id).first()
                if row is None:
                    slot = res.processed.slot if kind == "processed" else 0
                    BundleOutcome.objects.create(
                        bundle_uuid=res.bundle_id,
                        status=kind or "unknown",
                        landed=landed,
                        slot=slot,
                    )
                else:
                    if row.landed and not landed:
                        continue
                    row.status = kind or "unknown"
                    row.landed = landed or row.landed
                    if kind == "processed":
                        row.slot = res.processed.slot
                    row.save()
                PendingBundle.objects.filter(bundle_uuid=res.bundle_id).update(status=kind or "unknown")
        finally:
            close_old_connections()
