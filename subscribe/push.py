# -*- coding: utf-8 -*-

# @Author  : wzdnzd
# @Time    : 2022-07-15

import json
import os
import traceback
import urllib
import urllib.request
from contextlib import contextmanager
from http.client import HTTPResponse

import utils
from config.models import StorageConfig, StorageItem
from logger import logger
from urlvalidator import isurl

LOCAL_STORAGE = "local"


@contextmanager
def remote_write_policy(enabled: bool = True):
    """Block legacy/plugin writes during collection, including worker processes."""
    key = "AGGREGATOR_SKIP_REMOTE_PUSH"
    previous = os.environ.get(key)
    if not enabled:
        os.environ[key] = "1"
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = previous


class PushTo(object):
    def __init__(self, token: str = "", base: str = "", domain: str = "") -> None:
        base, domain = utils.trim(base), utils.trim(domain)
        if base and not domain:
            domain = base
        elif not base and domain:
            base = domain

        self.name = ""
        self.method = "PUT"
        self.domain = domain
        self.api_address = base
        self.token = "" if not token or not isinstance(token, str) else token

    def _storage(self, content: str, filename: str, folder: str = "") -> bool:
        if not content or not filename:
            return False

        basedir = os.path.abspath(os.environ.get("LOCAL_BASEDIR", ""))
        try:
            savepath = os.path.abspath(os.path.join(basedir, folder, filename))
            os.makedirs(os.path.dirname(savepath), exist_ok=True)
            with open(savepath, "w+", encoding="utf8") as f:
                f.write(content)
                f.flush()

            return True
        except:
            return False

    def push_file(self, filepath: str, item: StorageItem, group: str = "", retry: int = 5) -> bool:
        if not os.path.exists(filepath) or not os.path.isfile(filepath):
            logger.error(f"[PushFileError] file {filepath} not found")
            return False

        content = " "
        with open(filepath, "r", encoding="utf8") as f:
            content = f.read()

        return self.push_to(content=content, item=item, group=group, retry=retry)

    def push_to(self, content: str, item: StorageItem, group: str = "", retry: int = 5, **kwargs: object) -> bool:
        if os.environ.get("AGGREGATOR_SKIP_REMOTE_PUSH") == "1":
            logger.info("[PushReadOnly] skipped remote write during collection")
            return False
        if not self.validate(item=item):
            logger.error(f"[PushError] push config is invalidate, domain: {self.name}")
            return False

        if item.local:
            self._storage(content=content, filename=item.local)

        url, data, headers = self._generate_payload(content=content, item=item)
        payload = kwargs.get("payload", None)
        if payload and isinstance(payload, dict):
            try:
                data = json.dumps(payload).encode("UTF8")
            except:
                logger.error(f"[PushError] invalid payload, domain: {self.name}")
                return False

        try:
            request = urllib.request.Request(url=url, data=data, headers=headers, method=self.method)
            response = urllib.request.urlopen(request, timeout=60, context=utils.CTX)
            if self._is_success(response):
                logger.info(f"[PushSuccess] push subscribes information to {self.name} successed, group=[{group}]")
                return True
            else:
                logger.info("[PushError] remote storage rejected request, status=%s", response.getcode())
                return False

        except Exception as e:
            try:
                if isinstance(e, urllib.error.HTTPError):
                    code = getattr(e, "code", None)
                    # Responses may echo subscription tokens or the posted node list.
                    logger.error("[PushError] request failed, status=%s", code)
            except Exception:
                logger.error(f"[PushError] failed to process exception: {traceback.format_exc()}")

            self._error_handler(group=group)

            retry -= 1
            if retry > 0:
                return self.push_to(content, item, group, retry)

            return False

    def _is_success(self, response: HTTPResponse) -> bool:
        return response and response.getcode() == 200

    def _generate_payload(self, content: str, item: StorageItem) -> tuple[str, str, dict]:
        raise NotImplementedError

    def _error_handler(self, group: str = "") -> None:
        logger.error("[PushError] remote storage request failed")

    def validate(self, item: StorageItem) -> bool:
        raise NotImplementedError

    def filter_push(self, items: dict[str, StorageItem]) -> dict[str, StorageItem]:
        raise NotImplementedError

    def raw_url(self, item: StorageItem) -> str:
        raise NotImplementedError










class PushToLocal(PushTo):
    def __init__(self) -> None:
        super().__init__(token="")
        self.name = "local"

    def validate(self, item: StorageItem) -> bool:
        return isinstance(item, StorageItem) and bool(item.file_id)

    def push_to(self, content: str, item: StorageItem, group: str = "", retry: int = 5) -> bool:
        folder = item.folder_id
        filename = item.file_id
        success = self._storage(content=content, filename=filename, folder=folder)
        message = "successed" if success else "failed"
        logger.info(f"[PushInfo] push subscribes information to {self.name} {message}, group=[{group}]")
        return success

    def filter_push(self, items: dict[str, StorageItem]) -> dict[str, StorageItem]:
        return {k: v for k, v in items.items() if isinstance(v, StorageItem) and v.file_id}

    def raw_url(self, item: StorageItem) -> str:
        if not isinstance(item, StorageItem):
            return ""

        file_id = item.file_id
        folder_id = item.folder_id
        filepath = os.path.abspath(os.path.join(folder_id, file_id))
        return f"{utils.FILEPATH_PROTOCAL}{filepath}"


class PushToGist(PushTo):
    def __init__(self, token: str) -> None:
        super().__init__(token=token)

        self.name = "gist"
        self.api_address = "https://api.github.com/gists"
        self.domain = "https://gist.githubusercontent.com"
        self.method = "PATCH"

    def validate(self, item: StorageItem) -> bool:
        if not isinstance(item, StorageItem):
            return False

        gist_id = item.gist_id
        filename = item.filename

        return "" != self.token.strip() and "" != gist_id.strip() and "" != filename.strip()

    def _generate_payload(self, content: str, item: StorageItem) -> tuple[str, str, dict]:
        gist_id = item.gist_id
        filename = item.filename

        url = f"{self.api_address}/{gist_id}"
        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": utils.USER_AGENT,
        }

        data = json.dumps({"files": {filename: {"content": content, "filename": filename}}}).encode("UTF8")
        return url, data, headers

    def _is_success(self, response: HTTPResponse) -> bool:
        return response and response.getcode() == 200

    def filter_push(self, items: dict[str, StorageItem]) -> dict[str, StorageItem]:
        if not self.token or not isinstance(items, dict):
            return {}

        return {k: v for k, v in items.items() if k and isinstance(v, StorageItem) and v.gist_id and v.filename}

    def raw_url(self, item: StorageItem) -> str:
        if not isinstance(item, StorageItem):
            return ""

        username = utils.trim(item.username)
        gist_id = utils.trim(item.gist_id)
        revision = utils.trim(item.revision)
        filename = utils.trim(item.filename)

        if not username or not gist_id or not filename:
            return ""

        prefix = f"{self.domain}/{username}/{gist_id}"
        if revision:
            return f"{prefix}/raw/{revision}/{filename}"

        return f"{prefix}/raw/{filename}"




SUPPORTED_ENGINES = set(["gist"] + [LOCAL_STORAGE])


def get_instance(storage: StorageConfig) -> PushTo:
    if not isinstance(storage, StorageConfig):
        raise ValueError("[PushError] invalid storage config")

    engine = utils.trim(storage.engine)
    if engine not in SUPPORTED_ENGINES:
        raise ValueError(f"[PushError] unknown storge type: {engine}")

    token = utils.trim(storage.token or os.environ.get("PUSH_TOKEN", ""))
    if engine != LOCAL_STORAGE and not token:
        raise ValueError("[PushError] not found 'PUSH_TOKEN' in environment variables, please check it and try again")

    if engine == "gist":
        return PushToGist(token=token)

    return PushToLocal()
