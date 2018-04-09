'''
************************************************************************
FOR THE TIME BEING WHATEVER MODIFICATIONS ARE APPLIED TO THIS FILE
SHOULD ALSO BE APPLIED TO sdk_repository IN ANY OTHER PARTNER REPOS
************************************************************************
'''
import json
import logging
import os
import urllib.request
from itertools import chain
from typing import Callable

import sdk_cmd
import sdk_security
import shakedown
from retrying import retry
from sdk_utils import random_string, is_repo_supported

log = logging.getLogger(__name__)

DCOS_FILES_PATH = 'DCOS_FILES_PATH'
PACKAGE_REGISTRY_ENABLED = 'PACKAGE_REGISTRY_ENABLED'
PACKAGE_REGISTRY_NAME = 'package-registry'


def flatmap(f, items):
    """
    lines = ["one,two", "three", "four,five"]
    f     = lambda s: s.split(",")

    >>> map(f, lines)
    [['one', 'two'], ['three'], ['four', 'five']]

    >>> flatmap(f, lines)
    ['one', 'two', 'three', 'four', 'five']
    """
    return chain.from_iterable(map(f, items))


def parse_stub_universe_url_string(stub_universe_url_string) -> list:
    """Handles newline- and comma-separated strings."""
    lines = stub_universe_url_string.split("\n")
    return list(filter(None, flatmap(lambda s: s.split(","), lines)))


def get_universe_repos() -> list:
    # prepare needed universe repositories
    stub_universe_url_string = os.environ.get('STUB_UNIVERSE_URL', '')
    return parse_stub_universe_url_string(stub_universe_url_string)


def add_stub_universe_urls(stub_universe_urls: list) -> dict:
    stub_urls = {}

    if not stub_universe_urls:
        return stub_urls

    log.info('Adding stub URLs: {}'.format(stub_universe_urls))
    for idx, url in enumerate(stub_universe_urls):
        log.info('URL {}: {}'.format(idx, repr(url)))
        package_name = 'testpkg-{}'.format(random_string())
        stub_urls[package_name] = url

    # clean up any duplicate repositories
    current_universes = sdk_cmd.run_cli('package repo list --json')
    for repo in json.loads(current_universes)['repositories']:
        if repo['uri'] in stub_urls.values():
            log.info('Removing duplicate stub URL: {}'.format(repo['uri']))
            sdk_cmd.run_cli('package repo remove {}'.format(repo['name']))

    # add the needed universe repositories
    for name, url in stub_urls.items():
        log.info('Adding stub repo {} URL: {}'.format(name, url))
        rc, stdout, stderr = sdk_cmd.run_raw_cli(
            'package repo add --index=0 {} {}'.format(name, url))
        if rc != 0 or stderr:
            raise Exception(
                'Failed to add stub repo {} ({}): stdout=[{}], stderr=[{}]'.format(
                    name, url, stdout, stderr))

    log.info('Finished adding universe repos')

    return stub_urls


def remove_universe_repos(stub_urls):
    log.info('Removing universe repos')

    # clear out the added universe repositories at testing end
    for name, url in stub_urls.items():
        log.info('Removing stub URL: {}'.format(url))
        rc, stdout, stderr = sdk_cmd.run_raw_cli('package repo remove {}'.format(name))
        if rc != 0 or stderr:
            if stderr.endswith('is not present in the list'):
                # tried to remove something that wasn't there, move on.
                pass
            else:
                raise Exception('Failed to remove stub repo: stdout=[{}], stderr=[{}]'.format(stdout, stderr))

    log.info('Finished removing universe repos')


def add_dcos_files_to_registry(
        temp_dir
) -> None:
    # If DCOS_FILES_PATH is set, use the path OR use pytest's tmp file system.
    dcos_files_path = os.environ[DCOS_FILES_PATH]
    if not os.path.isdir(dcos_files_path):
        dcos_files_path = str(temp_dir)
    stub_universe_urls = get_universe_repos()
    log.info('Using {} to build .dcos files (if not exists) from {}'.format(
        dcos_files_path, stub_universe_urls
    ))
    dcos_files_list = build_dcos_files_from_stubs(
        stub_universe_urls,
        dcos_files_path,
        temp_dir
    )
    log.info('Bundled .dcos files : {}'.format(dcos_files_list))
    for file in dcos_files_list:
        rc, out, err = sdk_cmd.run_raw_cli(' '.join([
            'registry',
            'add',
            '--dcos-file={}'.format(file),
            '--json'
        ]))
        assert rc == 0
        assert len(json.loads(out)['packages']) > 0


def build_dcos_files_from_stubs(
        stub_universe_urls: list,
        dcos_files_path: str,
        temp_dir
) -> list:
    if not len(stub_universe_urls):
        return stub_universe_urls
    package_file_paths = []
    for repo_url in stub_universe_urls:
        headers = {
            "User-Agent": "dcos/{}".format(sdk_cmd.dcos_version()),
            "Accept": "application/vnd.dcos.universe.repo+json;"
                      "charset=utf-8;version={}".format('v4'),
        }
        req = urllib.request.Request(repo_url, headers=headers)
        with urllib.request.urlopen(req) as f:
            data = json.loads(f.read().decode())
            for package in data['packages']:
                package_file_paths.append(
                    build_dcos_file_from_universe_definition(
                        package,
                        dcos_files_path,
                        temp_dir
                    )
                )
    return package_file_paths


def build_dcos_file_from_universe_definition(
        package: dict,
        dcos_files_path: str,
        temp_dir
) -> str:
    # TODO Clean this up.
    del package['releaseVersion']
    del package['selected']
    package_json_file = temp_dir.mkdtemp().join(random_string())
    package_json_file.write(json.dumps(package))
    target = os.path.join(
        dcos_files_path,
        '{}-{}.dcos'.format(package['name'], package['version'])
    )
    rc, out, err = sdk_cmd.run_raw_cli(' '.join([
        'registry',
        'build',
        '--build-definition-file={}'.format(str(package_json_file)),
        '--output-directory={}'.format(dcos_files_path),
        '--json'
    ]))
    assert (rc == 0) or (rc == 1 and 'already exists' in out)
    assert os.path.isfile(target)
    return target


def install_package_registry(
        service_secret_path: str,
        temp_dir
) -> dict:
    repo_url = 'https://registry.marathon.l4lb.thisdcos.directory/repo'
    # TODO Add knobs to control local vs S3 storage.
    options = {
        'registry': {
            'service-account-secret-path': service_secret_path
        }
    }

    service_options = temp_dir.mkdtemp().join(random_string())
    service_options.write(json.dumps(options))

    # Install Package Registry
    rc, _, _ = sdk_cmd.run_raw_cli(
        'package install {} --options={} --yes'.format(
            PACKAGE_REGISTRY_NAME,
            str(service_options)
        )
    )
    assert rc == 0

    # Loop until repo is successfully added
    repo_name = 'package-registry-repo-{}'.format(random_string())
    loop_until_cli_condition(
        'package repo add --index=0 {} {}'.format(repo_name, repo_url),
        lambda code, out, err: code == 0
    )

    # If `describe` endpoint is working, registry is writable by AR.
    package_name = 'hello'
    package_version = 'world'
    expected_msg = 'Version [{}] of package [{}] ' \
                   'not found'.format(package_version, package_name)
    loop_until_cli_condition(
        'registry describe --package-name={} --package-version={}'.format(
            package_name, package_version
        ),
        lambda code, out, err: code == 1 and expected_msg in err
    )
    return {repo_name: repo_url}


def add_package_registry_stub() -> dict:
    stub_url = os.environ['PACKAGE_REGISTRY_STUB_URL']
    with urllib.request.urlopen(stub_url) as url:
        repo = json.loads(url.read().decode())
        min_supported = repo["packages"][0]["minDcosReleaseVersion"]

    version = sdk_cmd.dcos_version()

    if not is_repo_supported(version, min_supported):
        raise Exception('DC/OS {} does not support package registry. '
                        'Minimum required {}'.format(version, min_supported))
    return add_stub_universe_urls([stub_url])


def create_service_account_and_grant_perms(service_uid, service_secret_path):
    # Create service account for package registry
    sdk_security.create_service_account(service_uid, service_secret_path)
    sdk_cmd.run_raw_cli("security org users grant {} '{}' '{}'".format(
        service_uid,
        'dcos:adminrouter:ops:ca:rw',
        'full'))


@retry(
    stop_max_delay=5 * 60 * 1000,
    wait_fixed=5 * 1000
)
def loop_until_cli_condition(
        raw_cli_cmd: str,
        check: Callable[[int, str, str], bool],
) -> None:
    code, stdout, stderr = sdk_cmd.run_raw_cli(raw_cli_cmd)
    assert check(code, stdout, stderr)


def universe_session(temp_dir):
    """Add the universe package repositories defined in $STUB_UNIVERSE_URL.

    This should generally be used as a fixture in a framework's conftest.py:

    @pytest.fixture(scope='session')
    def configure_universe(tmpdir):
        yield from sdk_repository.universe_session(tmpdir)
    """
    stub_urls = {}
    package_registry_enabled = os.environ[PACKAGE_REGISTRY_ENABLED] == 'true'
    try:
        if package_registry_enabled:
            # TODO Remove stub. We should install from bootstrap registry.
            stub_urls = add_package_registry_stub()
            service_uid = 'pkg-reg-uid-{}'.format(random_string())
            service_secret_path = '{}-{}'.format(service_uid, random_string())
            create_service_account_and_grant_perms(service_uid,
                                                   service_secret_path)
            stub_urls = {
                **stub_urls,
                **install_package_registry(service_secret_path, temp_dir)
            }
            add_dcos_files_to_registry(temp_dir)
        else:
            stub_urls = add_stub_universe_urls(get_universe_repos())
        log.info('Set up universe_session successfully')
        yield
    finally:
        remove_universe_repos(stub_urls)
        if package_registry_enabled:
            log.info('Uninstalling package registry')
            shakedown.uninstall_package_and_wait(
                PACKAGE_REGISTRY_NAME,
                all_instances=True
            )
            # No need to revoke perms, just delete the secret.
            sdk_security.delete_service_account(service_uid,
                                                service_secret_path)

