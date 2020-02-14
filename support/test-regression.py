#! /usr/bin/env python3

import os
import sys
import tempfile
import xml.etree.ElementTree as ET


def run_and_analyse():
    with tempfile.NamedTemporaryFile() as test_results:
        os.system(
            "{} --gtest_output=xml:{} {}".format(
                TEST_PROGRAM, test_results.name, " ".join(sys.argv[3:])
            )
        )

        tree = ET.parse(test_results.name)
        testsuites = tree.getroot()

        known_failing_tests = set()
        known_failing_tests_name = "{}_failing".format(os.path.basename(TEST_PROGRAM))
        known_failing_tests_path = os.path.join(SRC_DIR, known_failing_tests_name)
        if os.path.isfile(known_failing_tests_path):
            with open(known_failing_tests_path, "r") as known_failing_tests_file:
                for test in known_failing_tests_file.readlines():
                    known_failing_tests.add(test.strip())

        failing_tests = set()
        for testsuite in testsuites:
            for test in testsuite:
                test_name = "{}.{}".format(
                    testsuite.attrib["name"], test.attrib["name"]
                )
                if (
                    not list(test)
                    and test.attrib["status"] == "run"
                    and test_name in known_failing_tests
                ):
                    print(
                        "This test is not failing please remove it from {} : {}".format(
                            known_failing_tests_name, test_name,
                        )
                    )
                if list(test):
                    failing_tests.add(test_name)

        should_not_fail_tests = failing_tests - known_failing_tests
        if len(should_not_fail_tests) != 0:
            print(
                "THESE TESTS SHOULD NOT HAVE FAILED: \n{}".format(
                    "\n".join(list(should_not_fail_tests))
                )
            )
            sys.exit(1)


if __name__ == "__main__":
    SRC_DIR = sys.argv[1]
    TEST_PROGRAM = sys.argv[2]

    run_and_analyse()
