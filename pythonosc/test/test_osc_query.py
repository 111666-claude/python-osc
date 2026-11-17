import http.client
import json
import threading
import unittest

from pythonosc.dispatcher import Dispatcher
from pythonosc.osc_message_builder import OscMessageBuilder
from pythonosc.osc_query import (
    OscQueryError,
    OscQueryMetadata,
    OscQueryService,
    OscQueryTreeTooLargeError,
    build_address_tree,
)


def _noop(address, *args):
    pass


class TestAddressTree(unittest.TestCase):
    def setUp(self):
        self.dispatcher = Dispatcher()

    def test_two_level_tree_from_explicit_registrations(self):
        self.dispatcher.map("/transport/play", _noop)
        self.dispatcher.map("/transport/stop", _noop)
        self.dispatcher.map("/mixer/gain", _noop)

        tree = self.dispatcher.oscquery_snapshot()
        root = build_address_tree(*tree)

        self.assertEqual("/", root["FULL_PATH"])
        self.assertEqual(
            {"mixer", "transport"}, set(root["CONTENTS"].keys())
        )
        transport = root["CONTENTS"]["transport"]
        self.assertEqual("/transport", transport["FULL_PATH"])
        self.assertEqual(
            {"/transport/play", "/transport/stop"},
            {c["FULL_PATH"] for c in transport["CONTENTS"].values()},
        )
        # Leaves carry no CONTENTS key.
        self.assertNotIn("CONTENTS", transport["CONTENTS"]["play"])

    def test_metadata_is_exposed_on_registered_leaf(self):
        self.dispatcher.map(
            "/mixer/gain",
            _noop,
            oscquery_type="f",
            oscquery_description="Channel gain",
            oscquery_access="readwrite",
        )
        addresses, metadata = self.dispatcher.oscquery_snapshot()
        self.assertEqual(["/mixer/gain"], addresses)
        self.assertEqual(
            OscQueryMetadata("f", "Channel gain", "readwrite"),
            metadata["/mixer/gain"],
        )
        leaf = build_address_tree(addresses, metadata)["CONTENTS"]["mixer"][
            "CONTENTS"
        ]["gain"]
        self.assertEqual("f", leaf["TYPE"])
        self.assertEqual("Channel gain", leaf["DESCRIPTION"])
        self.assertEqual("readwrite", leaf["ACCESS"])

    def test_identical_metadata_reregistration_is_accepted(self):
        self.dispatcher.map("/a/b", _noop, oscquery_type="f")
        self.dispatcher.map("/a/b", _noop, oscquery_type="f")
        _, metadata = self.dispatcher.oscquery_snapshot()
        self.assertEqual(OscQueryMetadata("f", None, None), metadata["/a/b"])

    def test_conflicting_metadata_is_rejected(self):
        self.dispatcher.map("/a/b", _noop, oscquery_type="f")
        with self.assertRaises(ValueError):
            self.dispatcher.map("/a/b", _noop, oscquery_type="i")
        with self.assertRaises(ValueError):
            self.dispatcher.map(
                "/a/b", _noop, oscquery_type="f", oscquery_access="read"
            )

    def test_invalid_access_value_is_rejected(self):
        with self.assertRaises(ValueError):
            self.dispatcher.map("/a/b", _noop, oscquery_access="owner")

    def test_invalid_addresses_are_rejected_with_metadata(self):
        for bad in (
            "",
            "no-leading-slash",
            "/",
            "/trailing/",
            "/double//slash",
            "/with space",
            "/with#hash",
            "/with\ttab",
            "/unicodé",
        ):
            with self.subTest(address=bad):
                with self.assertRaises(ValueError):
                    self.dispatcher.map(bad, _noop, oscquery_type="f")

    def test_wildcard_addresses_have_stable_literal_representation(self):
        self.dispatcher.map("/synth/*/volume", _noop, oscquery_type="f")
        self.dispatcher.map("/synth/bass/cutoff", _noop)
        addresses, metadata = self.dispatcher.oscquery_snapshot()
        first = build_address_tree(addresses, metadata)
        second = build_address_tree(addresses, metadata)
        self.assertEqual(first, second)
        self.assertEqual(
            json.dumps(first, sort_keys=True), json.dumps(second, sort_keys=True)
        )
        synth = first["CONTENTS"]["synth"]["CONTENTS"]
        self.assertIn("*", synth)
        self.assertEqual("/synth/*", synth["*"]["FULL_PATH"])
        self.assertEqual("f", synth["*"]["CONTENTS"]["volume"]["TYPE"])

    def test_unmap_removes_address_and_metadata(self):
        handler = self.dispatcher.map("/a/b", _noop, oscquery_type="f")
        self.dispatcher.unmap("/a/b", handler)
        addresses, metadata = self.dispatcher.oscquery_snapshot()
        self.assertEqual([], addresses)
        self.assertEqual({}, metadata)

    def test_unmap_one_of_several_handlers_keeps_metadata(self):
        handler = self.dispatcher.map("/a/b", _noop, oscquery_type="f")
        self.dispatcher.map("/a/b", _noop)
        self.dispatcher.unmap("/a/b", handler)
        addresses, metadata = self.dispatcher.oscquery_snapshot()
        self.assertEqual(["/a/b"], addresses)
        self.assertIn("/a/b", metadata)

    def test_tree_too_large_has_defined_error(self):
        for i in range(5):
            self.dispatcher.map(f"/big/{i}", _noop)
        addresses, metadata = self.dispatcher.oscquery_snapshot()
        with self.assertRaises(OscQueryTreeTooLargeError):
            build_address_tree(addresses, metadata, max_nodes=3)


class TestOscQueryService(unittest.TestCase):
    def setUp(self):
        self.dispatcher = Dispatcher()
        self.service = OscQueryService(self.dispatcher)
        self.service.start()
        self.addCleanup(self.service.stop)

    def _get(self, path):
        conn = http.client.HTTPConnection("127.0.0.1", self.service.port, timeout=5)
        try:
            conn.request("GET", path)
            response = conn.getresponse()
            body = response.read()
            return response.status, json.loads(body)
        finally:
            conn.close()

    def test_query_two_level_tree_over_http(self):
        self.dispatcher.map("/transport/play", _noop)
        self.dispatcher.map(
            "/transport/tempo",
            _noop,
            oscquery_type="f",
            oscquery_description="Tempo in BPM",
            oscquery_access="readwrite",
        )

        status, root = self._get("/")
        self.assertEqual(200, status)
        self.assertEqual("/", root["FULL_PATH"])
        self.assertIn("transport", root["CONTENTS"])

        status, subtree = self._get("/transport")
        self.assertEqual(200, status)
        self.assertEqual("/transport", subtree["FULL_PATH"])
        self.assertEqual({"play", "tempo"}, set(subtree["CONTENTS"].keys()))

        status, leaf = self._get("/transport/tempo")
        self.assertEqual(200, status)
        self.assertEqual("f", leaf["TYPE"])
        self.assertEqual("Tempo in BPM", leaf["DESCRIPTION"])
        self.assertEqual("readwrite", leaf["ACCESS"])

    def test_registration_updates_are_reflected(self):
        self.dispatcher.map("/a/one", _noop)
        status, payload = self._get("/a/one")
        self.assertEqual(200, status)

        handler = self.dispatcher.map("/a/two", _noop)
        status, payload = self._get("/a")
        self.assertEqual({"one", "two"}, set(payload["CONTENTS"].keys()))

        self.dispatcher.unmap("/a/two", handler)
        status, payload = self._get("/a/two")
        self.assertEqual(404, status)
        status, payload = self._get("/a")
        self.assertEqual({"one"}, set(payload["CONTENTS"].keys()))

    def test_unknown_path_returns_404(self):
        self.dispatcher.map("/a/b", _noop)
        for path in ("/nope", "/a/nope", "/a/b/deeper"):
            with self.subTest(path=path):
                status, payload = self._get(path)
                self.assertEqual(404, status)
                self.assertIn("error", payload)

    def test_invalid_query_path_returns_400(self):
        for path in (
            "/has%20space",
            "/double//slash",
            "/trailing/",
            "/wildcard*query",
            "/bracket[0]query",
            "/brace{x}query",
        ):
            with self.subTest(path=path):
                conn = http.client.HTTPConnection(
                    "127.0.0.1", self.service.port, timeout=5
                )
                try:
                    conn.request("GET", path)
                    response = conn.getresponse()
                    response.read()
                    self.assertEqual(400, response.status)
                finally:
                    conn.close()

    def test_non_get_methods_are_rejected(self):
        conn = http.client.HTTPConnection("127.0.0.1", self.service.port, timeout=5)
        try:
            conn.request("POST", "/", body="{}")
            response = conn.getresponse()
            response.read()
            self.assertEqual(405, response.status)
        finally:
            conn.close()

    def test_queries_never_invoke_control_callbacks(self):
        calls = []
        self.dispatcher.map("/ctl/fader", lambda address, *a: calls.append(address))
        for path in ("/", "/ctl", "/ctl/fader"):
            status, _ = self._get(path)
            self.assertEqual(200, status)
        self.assertEqual([], calls)

    def test_osc_dispatch_is_unaffected_by_running_service(self):
        calls = []
        self.dispatcher.map("/ctl/fader", lambda address, *a: calls.append(a))
        builder = OscMessageBuilder("/ctl/fader")
        builder.add_arg(0.5)
        self.dispatcher.call_handlers_for_packet(
            builder.build().dgram, ("127.0.0.1", 9999)
        )
        self.assertEqual([(0.5,)], calls)
        status, _ = self._get("/ctl/fader")
        self.assertEqual(200, status)

    def test_concurrent_registration_and_queries(self):
        errors = []
        stop = threading.Event()

        def register():
            try:
                for i in range(100):
                    self.dispatcher.map(f"/dyn/{i % 10}/param", _noop)
                    self.dispatcher.unmap(f"/dyn/{i % 10}/param", _noop)
            except Exception as exc:  # pragma: no cover - failure path
                errors.append(exc)
            finally:
                stop.set()

        def query():
            while not stop.is_set():
                try:
                    conn = http.client.HTTPConnection(
                        "127.0.0.1", self.service.port, timeout=5
                    )
                    try:
                        conn.request("GET", "/")
                        response = conn.getresponse()
                        self.assertEqual(200, response.status)
                        json.loads(response.read())
                    finally:
                        conn.close()
                except Exception as exc:  # pragma: no cover - failure path
                    errors.append(exc)
                    return

        threads = [
            threading.Thread(target=register),
            threading.Thread(target=register),
            threading.Thread(target=query),
            threading.Thread(target=query),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        self.assertEqual([], errors)

    def test_huge_tree_returns_413(self):
        service = OscQueryService(self.dispatcher, max_nodes=4)
        service.start()
        self.addCleanup(service.stop)
        for i in range(10):
            self.dispatcher.map(f"/huge/{i}", _noop)
        conn = http.client.HTTPConnection("127.0.0.1", service.port, timeout=5)
        try:
            conn.request("GET", "/")
            response = conn.getresponse()
            payload = json.loads(response.read())
            self.assertEqual(413, response.status)
            self.assertIn("error", payload)
        finally:
            conn.close()


class TestServiceLifecycle(unittest.TestCase):
    def test_start_twice_raises(self):
        service = OscQueryService(Dispatcher())
        service.start()
        try:
            with self.assertRaises(OscQueryError):
                service.start()
        finally:
            service.stop()

    def test_stop_releases_port_and_is_idempotent(self):
        service = OscQueryService(Dispatcher())
        service.start()
        port = service.port
        self.assertTrue(service.is_running)
        service.stop()
        service.stop()  # no-op
        self.assertFalse(service.is_running)
        with self.assertRaises(OscQueryError):
            service.url
        with self.assertRaises(OscQueryError):
            service.port
        with self.assertRaises(OSError):
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
            conn.request("GET", "/")
            conn.getresponse()

    def test_restart_after_stop(self):
        dispatcher = Dispatcher()
        dispatcher.map("/a/b", _noop)
        service = OscQueryService(dispatcher)
        service.start()
        service.stop()
        service.start()
        try:
            conn = http.client.HTTPConnection(
                "127.0.0.1", service.port, timeout=5
            )
            try:
                conn.request("GET", "/a/b")
                response = conn.getresponse()
                self.assertEqual(200, response.status)
                response.read()
            finally:
                conn.close()
        finally:
            service.stop()

    def test_context_manager(self):
        with OscQueryService(Dispatcher()) as service:
            self.assertTrue(service.is_running)
        self.assertFalse(service.is_running)

    def test_invalid_construction_arguments(self):
        with self.assertRaises(ValueError):
            OscQueryService(None)
        with self.assertRaises(ValueError):
            OscQueryService(Dispatcher(), max_nodes=0)


if __name__ == "__main__":
    unittest.main()
