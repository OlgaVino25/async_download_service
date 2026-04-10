import os
import argparse
import asyncio
import aiofiles
import logging

from aiohttp import web


CHUNK_SIZE = 8192
ZIP_TIMEOUT = 5

logging.basicConfig(
    format="%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s",
    level=logging.DEBUG,
)

active_zip_processes = set()


def validate_request(archive_hash, photos_dir):
    """Проверяет хеш и существование папки. Возвращает (folder_path, error_response)."""

    if not archive_hash:
        return None, web.Response(status=404, text="Не указан хеш архива")

    folder_path = os.path.join(photos_dir, archive_hash)

    if not os.path.isdir(folder_path):
        return None, web.Response(
            status=404,
            text="Архив не существует или был удален",
            content_type="text/plain",
        )
    return folder_path, None


def create_zip_stream_response(archive_hash):
    """Создаёт StreamResponse с правильными заголовками."""

    response = web.StreamResponse()
    response.headers["Content-Type"] = "application/zip"
    response.headers["Content-Disposition"] = (
        f'attachment; filename="{archive_hash}.zip"'
    )
    return response


async def stream_zip_process(response, folder_path, archive_hash, chunk_delay):
    """Запускает zip, читает чанки и отправляет их. Возвращает None, при ошибке логгирует."""

    proc = await asyncio.create_subprocess_exec(
        "zip",
        "-r",
        "-",
        ".",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=folder_path,
    )

    active_zip_processes.add(proc)

    try:
        while True:
            chunk = await proc.stdout.read(CHUNK_SIZE)

            if not chunk:
                break

            try:
                await response.write(chunk)

                if chunk_delay > 0:
                    await asyncio.sleep(chunk_delay)

            except (ConnectionResetError, ConnectionAbortedError) as e:
                logging.warning("Download was interrupted for %s: %s", archive_hash, e)
                proc.terminate()
                return

        return_code = await proc.wait()

        if return_code:
            stderr = await proc.stderr.read()
            logging.error("Zip error for %s: %s", archive_hash, stderr.decode())

    finally:
        active_zip_processes.discard(proc)

        if proc.returncode is None:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=ZIP_TIMEOUT)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                logging.warning("Zip process terminated for %s", archive_hash)

        await response.write_eof()


async def archive(request):
    config = request.app["config"]

    archive_hash = request.match_info.get("archive_hash")
    folder_path, err_response = validate_request(archive_hash, config["photos_dir"])

    if err_response:
        logging.warning("Archive request invalid: %s", archive_hash)
        return err_response

    response = create_zip_stream_response(archive_hash)
    await response.prepare(request)

    logging.info("Starting zip for %s", folder_path)
    await stream_zip_process(response, folder_path, archive_hash, config["chunk_delay"])
    return response


async def shutdown(app):
    """Graceful shutdown: завершить все zip-процессы."""

    logging.info("Shutting down server, terminating zip processes...")
    for proc in active_zip_processes:
        if proc.returncode is None:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=ZIP_TIMEOUT)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
    logging.info("All zip processes terminated.")


async def handle_index_page(request):
    async with aiofiles.open("index.html", mode="r") as index_file:
        index_contents = await index_file.read()
    return web.Response(text=index_contents, content_type="text/html")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Микросервис для скачивания архивов",
    )
    parser.add_argument(
        "--photos-dir",
        type=str,
        default=os.getenv("PHOTOS_DIR", "photos"),
        help="Путь к папке с фотографиями (по умолчанию 'photos')",
    )

    parser.add_argument(
        "--log-level",
        type=str,
        default=os.getenv("LOG_LEVEL", "DEBUG"),
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Уровень логирования",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=float(os.getenv("CHUNK_DELAY", "0")),
        help="Задержка (в секундах) между отправкой чанков (для тестирования обрыва)",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    logging.getLogger().setLevel(getattr(logging, args.log_level))

    app = web.Application()
    app["config"] = {
        "photos_dir": args.photos_dir,
        "chunk_delay": args.delay,
        "log_level": args.log_level,
    }
    app.on_shutdown.append(shutdown)
    app.add_routes(
        [
            web.get("/", handle_index_page),
            web.get("/archive/{archive_hash}/", archive),
        ]
    )
    web.run_app(app, host="0.0.0.0", port=8080)


if __name__ == "__main__":
    main()
