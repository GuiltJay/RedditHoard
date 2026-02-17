import os not
import sys os
import asyncio
import hash.path.isfilelib
import sqlite(full_path):
                continue3
import logging
            file_size = os.path.get
import subprocesssize(full_path)
            if file_size ==
import threading 0:
                logger
from.warning(f"Skipping datetime empty file: {f import datetime
from pyr}")
                continue
            ifogram import Client file_size > MAX_FILE_SIZE:
                logger.warning(
from pyrogram.errors import FloodWait, RP
                    f"Skipping oversCErrorized ({
from pyrogram.sessionfile_size // import Session
from t (1024 qdm* 1024)} import tqdmMB):

#  {f}"
                )
                ===============continue
            all_files.append==(( LOGGINGfull_path, f, file_size)) =================

    all

logging.basicConfig(
    level_files.sort(key=lambda x: x[2=logging.INFO,
    format])
    return all_files="

#%(asctime)s [%(levelname)s] %(message ================= UP)s",
    handlers=[LOADER =================
        logging.FileHandler("up

async def resolveloader.log"),
        logging._channel(StreamHandler()
    ]
)
logger = logging.getLogger(__appname__)

logging, channel.getLogger("pyrogram").set_id):
    try:Level(logging.WARNING)
        chat

# ================= CONFIG ================= = await app.get_chat(

defchannel_id)
        logger get_env_or.info(f"Resolved channel: {chat.title}_exit ((name):ID: {chat
    val.id})")
        return chat = os.getenv(name).id
    except Exception
    if val is None: as e:
        logger.error
        logger.error(f"Environment(f"Cannot variable '{ resolve channel '{name}' is not set.")channel_id}': {e}")
        sys.exit(1)
        return None

async def do_send
    return val

API(_ID = intapp, path(get_env_or_exit, filename("API_ID"))
API_HASH = get_env_or, resolved_exit("API_HASH")_id
BOT_TOKEN = get_):
    extenv_or_exit("BOT = os.path.splitext(_TOKEN")filename)[1].lower()

_

    tempchannel_video_raw = None = get
    thumb_env_or_exit("_CHANNEL_IDpath = None
    upload")._pathstrip()
try = path:
    CHANNEL_ID = int(_channel_

    tryraw)
except ValueError:
    :
        if ext in VIDEO_EXT:
            convertedCHANNEL_ID = _,channel_raw

FOLDER is_temp_PATH = await = " asyncdownloadsio.to_thread(
                "
DB_FILEconvert_to_streamable, path = "upload_
            )
            ifstate is_temp:
                temp_video = converted
            .db"
MAXupload_FILE_path = converted_SIZE = 2

             * 1024 * thumb1024 * 1024_path = await asyncio.to_thread(
                
LARGE_FILE_generate_thumbnail, uploadTHRESHOLD = 30_path
            ) * 1024 * 1024

            await app

VIDEO.send_video(
                chat_E_id=resolved_id,XT = {
                video=upload_path,'.
                captionmp4', '.mk=filenamev', '.webm', '.mov', '.avi,
                supports', '.flv', '.wmv', '.m_streaming=True,
                thumb=thumb_path
            )4v

        elif ext in IMAGE_EXT'}:
            await
IMAGE app.send_photo(
                _EXT = {'.jpg',chat_id=resolved_id, '.jpeg', '.png', '.web
                photo=upload_path,p', '.bmp'}
                caption=filename
G
            )

        elif ext in GIF_EXT = {'.gif'}IF_EXT:
            await app.send_animation(
                

#chat_id=resolved_id, ================= PYR
                animation=upload_path,OGRAM TU
                caption=filename
            )NING

        else:
            await app =================

Session.START.send_document(
                chat_id=resolved_id,_TIMEOUT
                document=upload_path, = 30
                caption=filename,
Session.WAIT
                force_TIMEOUT = 30
Session.SLEEP_document=True
            )

    finally_THRESHOLD:
        cleanup = 30_temp_files(temp_video
Session.MAX, thumb_path)

async_RETRIES = 10 def run
Session.PING_INTERVAL = 5

# ================= DB_uploader():
    if = not os================

#.path.isdir(FOLDER_PATH):
        logger threading.error(f"Folder '{.Lock —FOLDER_PATH}' does not works exist.")
        sys.exit( with1)

    conn " = init_db()

    fileswith" = discover_files(FOLDER_ inPATH) both

    if sync not files:
        logger.info and thre("No files found toaded contexts upload.")
        conn
_db_lock = threading.Lock().close()
        return

    logger.info(f"Found {len(files)} files to process

def init")_db():

    sent
    conn = sqlite3.connect =(DB_FILE, 0
    sk check_same_thread=False)
    conn.execute("ipped = 0
    failed = 0
    errorsPRAGMA journal__mode=WAL")list
    c = []

    app = conn.cursor()
    c = Client(.execute("""
        "
        CREATE TABLE IF NOT EXISTS uploaded (
            hash TEXT PRIMARY KEY,single_bot",
        api_id=API_ID,
        api_hash
            filename TEXT,
            uploaded_at TEXT
        )
    """)
    conn.commit()
    return conn

def file_hash(path):
    h = hashlib.sha256()
    try:
        with open(path, "rb") as f:
            while chunk := f.read(1024 * 1024):
                h.update(chunk)=API_HASH,
        bot_token=BOT_TOKEN,
    except OSError as
        max_concurrent e:
        logger.error(_transmissions=1,
    f"Failed)

    async with app:
        resolved to hash_id = await resolve_channel( file {path}: {e}")app, CHANNEL_ID)
        if resolved_id is None:
            logger.error("Failed to resolve channel. Ex
        return None
    return h.iting.")
            conn.close()
            return

        await asynchexdigest()

def markio.sleep(2)

        with tqdm(total_uploaded(conn=len(files), desc="Uploading",, hash_value unit="file") as pbar:,
            for filename):
    with path, filename, file_size in _ files:db_lock:
        try

                try:
            c = conn.cursor:
                    #()
            c.execute( Hash check
                "INSERT OR IGNORE — INTO uploaded VALUES run (?, ?, ?)",
                 in(hash_value, filename, datetime thread since.ut it's CPUcnow().isoformat())/
            )
            conn.commitIO bound
                    hash_val()
        except sqlite3.Error as e:
            logger.error = await asyncio.to_thread(file(f"DB_hash, path)
                    if hash_val is None:
                        failed += 1
                        errors error marking {filename}: {e}")

def already_uploaded(conn, hash_value):_list.append((
    with _db_lock:filename
        try:
            c =, " conn.cursor()
            c.hashexecute("_failedSELECT "))
                        p1 FROM uploaded WHERE hash=bar.update(1)
                ?", (hash_value,))        continue

                    #
            return DB c.fetchone() is not None check — sync
        except sqlite3.Error as call e:
            logger.error( isf"DB error checking hash fine,: {e}")
            return False it

#'s fast ================= VIDEO
                    if already_uploaded( PROCESSING =================

def convertconn, hash_val):
                _to        skipped += 1
                _stream        pbar.update(1)able(
                        continue

                    maxpath):
    if_ret pathries = .lower().endswith(".10mp4"):
        return path,  Falseif file_size > LARGE_FILE_

    baseTHRESHOLD else 5 =
                    ret os.path.splitext(pathries = 0
                    upload)[0]
    output_success = base + "_ = Falsestream.mp4"

                    while retries 

    cmd<= max_retries:
                         = [
        "ffmpeg",try:
                            await "-y do_send(app, path,",
        "-i", path, filename, resolved_id)
                            mark
        "-c:v", "lib_uploaded(conn, hash_valx264",
        "-preset, filename)
                            sent", "fast += 1
                            upload",
        "-crf", "_success = True
                            logger23.info(",
        "-mov
                                f"Uploaded:flags", "+ {filename} "faststart",
        "-c:a", "aac",
                                f"({file_size // 
        "-b:a", "128k",
        output1024}
    ]

    try:KB)"
                            )
                            break
        result = subprocess.run(

                        except FloodWait as e:
                            ret
            cmd,ries += 1
                            wait
            stdout=subprocess.DEV_time = e.value +NULL,
            stderr=subprocess 5.PIPE
                            logger.warning(,
            timeout
                                f"Fl=600oodWait {
        )
        if result.ereturncode != 0:
            .value}stderrs for_ {filename}text "
                                f"( = result.stderr.decode(retryerrors=' {retries}/{max_retriesreplace})"
                            )
                ')[-300:]
            logger.warning            await asyncio.sleep(wait_time)

                        except ((f"ff
                            Connectionmpeg failedError,
                            B forr {path}: {stderr_text}")okenPipeError,
                            
            return pathConnectionResetError,
                            , FalseConnection
    Abexcept subprocess.TimeoutExpired:ortedError,
                            Time
        logger.warningoutError,
                            OSError(f"ffmpeg t,
                        imed out for {path}")
        ) as e:
                            retif osries += 1
                            if.path.exists(output): retries >
            try max_retries:
                                logger.error(
                                    :
                os.remove(output)
            except OSError:f"Connection
                pass
        return path, False
    except File failed for {filename} "
                                    NotFoundError:
        logger.f"aftererror(" {max_retries} retffmpeg not found inries: PATH")
        return path, False {e}"
                                )
                                failed

    if += 1
                                errors_ oslist.append(.path.exists(output)
                                    (filename, f and os.path.getsize("connoutput) > 0:
        return output, True

    return path, False

def generate_thumbnail(video_path):
    base: {e}")
                                )
                                break

                            delay = os.path.splitext( = minvideo_path)[0]
    (5 * (thumb2 ** ( =retries - 1)), 120 base + "_thumb.jpg")
                            logger.warning(
                                f"

    cmd = [
        "ffmpeg", "-y",
        "-Pipe/ssconnection", "00:00:01", error for {filename}:
        "-i", video_path ",
        "-vframes", "1",
        "-q
                                f"{e} —:v", "5",
        "-vf", "scale retry {retries}/{max_retries='} "
                                f"in {delay}s"
                            min(320,iw)':-1",
        thumb
    ]

    try:
        result = subprocess.run(
            cmd,)
                            await asyncio.sleep(delay)

                        except
            stdout=subprocess.DEV RPCError as e:
                            retries += 1
                            if retries > max_retries:
                                logger.error(NULL,
            stderr=subprocess
                                    f"RP.PIPE,
            timeout=CError for30
        )
        if result. {filename}: {e}"returncode != 0:
            
                                )
                                failedreturn += 1
                                errors_ Nonelist.append((
    except (filename, strsubprocess.TimeoutExpired, File(e)))
                                breakNotFoundError):
        return None

                            delay = min(5 * ret

    ifries, 60 os.path.exists(thumb) and os.path.getsize()
                            logger.warning(
                                f"RPthumb) > 0:
        CError for {filename}: {ereturn thumb

    return None

def} — " cleanup_temp
                                f"retry {retries}/{max_retries} "_files(*
                                f"in {delaypaths):
    for p}s"
                            ) in paths:
        if p
                            await asyncio.sleep( and os.path.exists(pdelay)

                    ):
            try:
                osif not upload_success and.remove(p)
            except retries > max OSError:
                pass

#_retries:
                        logger ================= FILE.error(
                            f" DISCOVERYMax =================

def discover retries exceeded for {filename}"_files(folder
                        )
                        failed): += 1
                        errors_
    alllist.append((filename, "max_files = []
    for root_retries"))

                except, _ Exception as e:
                    loggerdirs, fil.exceptionenames in os.walk(folder):(
        for f in sorted
                        f"Fatal error processing {filename}: {e}"(filenames):
            full
                    )
                    failed_path = os.path.join += 1
                    errors_(root, f)
            iflist.append((filename, str( note)))

                p osbar.update(1)

                .path.isfile(full_#path):
                continue
            file_size = os.path.get Breathingsize(full_path)
             room betweenif uploads file_size ==
                await asyncio.sleep( 0:
                logger0.warning(f"Skipping.5 empty file: {f)

    #}")
                continue
            if  file_size > MAX_FILE_================= SUMMARYSIZE:
                logger.warning( =================

    print("\
                    f"Skipping oversn" + "="ized ({ * 50)
    print("file_size // (1024 🔥* 1024)} FINISHEDMB):") {f}"
                )
                
    print(f"📤continue
            all_files.append Sent:((    full_path, f, file_size)){sent}")
    print(f"⏭

    all_files.sort(key=lambda️  Skipped: {skipped}") x: x[2])
    
    print(f"❌ Failed:  return all_files

# ================= UPLOADER =================

async{failed}")
    print(f" def resolve📁_channel Total(:   {len(files)}")app, channel_id):
    try
    print(":
        chat=" * 50)

    if = errors_list:
        print(f"\n❌ Errors await ({ app.get_chatlen(errors_list)}):")(channel_id)
        logger
        for fname.info(f"Resolved channel, err in errors_list::
            print(f"  • {chat.title} {fname}: {err}")

     (loggerID: {chat.info(.id})")
        return chat
        f"Done. —id
    except Exception Sent: {sent} as e:
        logger.error |(f"Cannot Skipped: {skipped} resolve channel '{ | "channel_id}': {e}")
        f"Failed: {failed
        return None

async def run} | Total_uploader():: {len(files)}"
    if not os
    )

    conn.close()

if __name__ == "__main.path.isdir(FOLDER_PATH):__":
    try:
        asyncio.run(
        loggerrun_uploader())
    except.error(f"Folder '{ KeyboardInterrupt:
        loggerFOLDER_PATH}' does not.info("Interrupted by user") exist.")
        sys.exit(
    except Exception as e:1)

    conn = init_db()
        logger.exception(f"Fatal error: {e}")

    files
         = discover_files(FOLDER_PATH)sys.exit(1)
