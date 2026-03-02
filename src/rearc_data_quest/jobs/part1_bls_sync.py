from __future__ import annotations

import hashlib
import logging
import mimetypes
from dataclasses import dataclass
from datetime import UTC, datetime
from html.parser import HTMLParser
from urllib.parse import unquote, urljoin, urlparse

from rearc_data_quest.aws_utils import s3_client
from rearc_data_quest.config import Settings
from rearc_data_quest.http_utils import HttpSession, build_session
from rearc_data_quest.logging_utils import configure_logging, get_run_id, set_run_id

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RemoteFile:
    name: str
    url: str


@dataclass(frozen=True)
class S3ObjectSummary:
    key: str
    size: int
    etag: str | None


@dataclass(frozen=True)
class SyncStats:
    discovered: int
    uploaded_new: int
    uploaded_updated: int
    skipped_unchanged: int
    deleted_stale: int

#_DirectoryListingParser reads the BLS directory webpage HTML 
# and collects all link targets (href values) so the script can discover which files are available.
class _DirectoryListingParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return

        for attr_name, attr_value in attrs:
            if attr_name.lower() == "href" and attr_value:
                self.hrefs.append(attr_value)
                return


def _normalize_s3_prefix(prefix: str) -> str:
    return prefix if prefix.endswith("/") else f"{prefix}/"

# return the cleaned file name in a list
def _parse_bls_listing(html: str, base_url: str) -> list[RemoteFile]:
    parser = _DirectoryListingParser()
    #feed belong to HTML parser class
    # and populated to self.href with handle startag
    parser.feed(html)

    #Breaks the full URL into parts (scheme, domain, path, etc.)
    parsed_base = urlparse(base_url)
    base_path = parsed_base.path if parsed_base.path.endswith("/") else f"{parsed_base.path}/"

    files: dict[str, RemoteFile] = {}
    for href in parser.hrefs:
        href = href.strip()
        if not href or href in {"../", "./"}:
            continue
        if href.startswith(("?", "#")):
            continue

        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)
        if parsed.scheme not in {"http", "https"}:
            continue
        if parsed.netloc != parsed_base.netloc:
            continue
        if parsed.path.endswith("/"):
            continue
        if not parsed.path.startswith(base_path):
            continue

        filename = unquote(parsed.path.rsplit("/", 1)[-1])
        if not filename:
            continue
        files[filename] = RemoteFile(name=filename, url=full_url)

    return [files[name] for name in sorted(files)]


def _list_remote_files(base_url: str, session: HttpSession) -> list[RemoteFile]:
    response = session.get(base_url, timeout=30)
    response.raise_for_status()
    files = _parse_bls_listing(response.text, base_url)
    if not files:
        raise RuntimeError(f"No files discovered at BLS URL: {base_url}")
    return files


def _list_s3_objects(s3, bucket: str, prefix: str) -> dict[str, S3ObjectSummary]:
    paginator = s3.get_paginator("list_objects_v2")
    result: dict[str, S3ObjectSummary] = {}

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            if not key.startswith(prefix):
                continue

            #removes the common folder part and only keeps direct files, not folder entries or nested subfolder files.
            relative_name = key[len(prefix) :]
            if not relative_name or "/" in relative_name:
                # Keep the prefix flat so delete logic only touches the expected files.
                continue
            
            #builds the current S3 state map
            #later sync logic uses it to check stage
            etag = obj.get("ETag")
            result[relative_name] = S3ObjectSummary(
                key=key,
                size=int(obj["Size"]),
                etag=etag.strip('"') if isinstance(etag, str) else None,
            )

    return result


def _download_file(file: RemoteFile, session: HttpSession) -> tuple[bytes, str]:
    response = session.get(file.url, timeout=120)
    response.raise_for_status()
    content = response.content
    content_md5 = hashlib.md5(content).hexdigest()
    return content, content_md5


def _existing_source_md5(s3, bucket: str, key: str, fallback_etag: str | None) -> str | None:
    head = s3.head_object(Bucket=bucket, Key=key)
    metadata = head.get("Metadata", {})
    return metadata.get("source_md5") or fallback_etag


def _sync_bls_to_s3(settings: Settings) -> SyncStats:
    prefix = _normalize_s3_prefix(settings.s3_bls_prefix)
    s3 = s3_client(settings.aws_region)

    logger.info(
        "Starting BLS sync",
        extra={
            "bucket": settings.s3_bucket_name,
            "prefix": prefix,
            "source_url": settings.bls_base_url,
        },
    )

    with build_session(settings.bls_user_agent) as session:
        remote_files = _list_remote_files(settings.bls_base_url, session)
        existing_objects = _list_s3_objects(s3, settings.s3_bucket_name, prefix)
        remote_names = {file.name for file in remote_files}

        uploaded_new = 0
        uploaded_updated = 0
        skipped_unchanged = 0

        for remote_file in remote_files:
            key = f"{prefix}{remote_file.name}"
            body, content_md5 = _download_file(remote_file, session)
            existing = existing_objects.get(remote_file.name)

            if existing is not None:
                existing_md5 = _existing_source_md5(
                    s3, settings.s3_bucket_name, existing.key, existing.etag
                )
                if existing_md5 == content_md5 and existing.size == len(body):
                    skipped_unchanged += 1
                    logger.debug("Skipping unchanged file: %s", remote_file.name)
                    continue

            content_type = mimetypes.guess_type(remote_file.name)[0] or "application/octet-stream"
            s3.put_object(
                Bucket=settings.s3_bucket_name,
                Key=key,
                Body=body,
                ContentType=content_type,
                Metadata={
                    "source_system": "bls",
                    "source_url": remote_file.url,
                    "source_md5": content_md5,
                    "synced_at_utc": datetime.now(UTC).isoformat(),
                    "pipeline_run_id": get_run_id(),
                },
            )

            if existing is None:
                uploaded_new += 1
                logger.info("Uploaded new file: %s", remote_file.name)
            else:
                uploaded_updated += 1
                logger.info("Uploaded updated file: %s", remote_file.name)

    deleted_stale = 0
    stale_names = sorted(set(existing_objects) - remote_names)
    for stale_name in stale_names:
        s3.delete_object(Bucket=settings.s3_bucket_name, Key=existing_objects[stale_name].key)
        deleted_stale += 1
        logger.info("Deleted stale file from S3: %s", stale_name)

    return SyncStats(
        discovered=len(remote_files),
        uploaded_new=uploaded_new,
        uploaded_updated=uploaded_updated,
        skipped_unchanged=skipped_unchanged,
        deleted_stale=deleted_stale,
    )


def run(*, run_id: str | None = None) -> None:
    configure_logging()
    if run_id:
        set_run_id(run_id)
    settings = Settings.from_env()
    stats = _sync_bls_to_s3(settings)
    logger.info(
        (
            "BLS sync complete | discovered=%s uploaded_new=%s uploaded_updated=%s "
            "skipped_unchanged=%s deleted_stale=%s"
        ),
        stats.discovered,
        stats.uploaded_new,
        stats.uploaded_updated,
        stats.skipped_unchanged,
        stats.deleted_stale,
    )


if __name__ == "__main__":
    run()
