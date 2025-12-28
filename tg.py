import os
import asyncio
import random
from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import InputMediaPhoto, InputMediaVideo, InputMediaDocument
from tqdm import tqdm

# File type mappings
IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.bmp', '.webp', '.tiff', '.tif'}
VIDEO_EXTENSIONS = {'.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v'}
GIF_EXTENSIONS = {'.gif'}
AUDIO_EXTENSIONS = {'.mp3', '.wav', '.ogg', '.flac', '.m4a', '.aac'}
VOICE_EXTENSIONS = {'.ogg'}  # Telegram voice messages typically use .ogg

def get_file_type(filename):
    """Determine the file type based on extension"""
    ext = os.path.splitext(filename)[1].lower()
    
    if ext in IMAGE_EXTENSIONS:
        return 'photo'
    elif ext in VIDEO_EXTENSIONS:
        return 'video'
    elif ext in GIF_EXTENSIONS:
        return 'animation'  # Telegram uses 'animation' for GIFs
    elif ext in AUDIO_EXTENSIONS:
        return 'audio'
    elif ext in VOICE_EXTENSIONS:
        return 'voice'
    else:
        return 'document'

async def send_files_to_telegram(folder_path, bot_token, channel_id, delay_between_sends=1.0, max_retries=3):
    """
    Sends all files from the specified folder to a Telegram channel using a Kurigram (Pyrogram fork) bot.
    Dynamically detects file types and uses appropriate sending methods.
    
    Args:
    - folder_path (str): Path to the folder containing files.
    - bot_token (str): Your Telegram bot token.
    - channel_id (str): The channel ID (e.g., '@channelusername' or numeric ID).
    - delay_between_sends (float): Delay in seconds between each file send (default 1.0 to avoid flood waits).
    - max_retries (int): Maximum retries per file on flood wait (default 3, with exponential backoff).
    
    Note: Ensure the bot is added as an admin to the channel.
    Files larger than 2000MB will be skipped (Telegram bot limit; 4000MB with Premium).
    """
    app = Client("uploader_bot", API_ID, API_HASH, bot_token=bot_token)
    
    if not os.path.exists(folder_path):
        print(f"Folder '{folder_path}' does not exist.")
        return
    
    # Collect list of valid files first for accurate progress tracking
    valid_files = []
    files_skipped_size = 0
    file_type_stats = {
        'photo': 0,
        'video': 0,
        'animation': 0,
        'audio': 0,
        'voice': 0,
        'document': 0
    }
    
    print("Scanning folder for valid files...")
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        
        if os.path.isfile(file_path):
            file_size = os.path.getsize(file_path)
            if file_size > 2000 * 1024 * 1024:  # 2000MB limit (updated for 2025 Bot API)
                print(f"Skipping {filename} - file too large ({file_size / (1024 * 1024):.2f} MB).")
                files_skipped_size += 1
                continue
            
            file_type = get_file_type(filename)
            valid_files.append((file_path, filename, file_type))
            file_type_stats[file_type] += 1
    
    total_files = len(valid_files)
    if total_files == 0:
        print("No valid files found to send.")
        return
    
    # Print file type statistics
    print("\n📊 File type distribution:")
    for file_type, count in file_type_stats.items():
        if count > 0:
            print(f"  {file_type.capitalize()}: {count} files")
    
    files_sent = 0
    files_skipped_error = 0
    
    async with app:
        # Progress bar for sending files
        with tqdm(total=total_files, desc="Sending files", unit="file") as pbar:
            for file_path, filename, file_type in valid_files:
                retry_count = 0
                success = False
                
                while retry_count <= max_retries and not success:
                    try:
                        if file_type == 'photo':
                            await app.send_photo(
                                chat_id=channel_id,
                                photo=file_path,
                                caption=f"📸 {filename}"
                            )
                        elif file_type == 'video':
                            await app.send_video(
                                chat_id=channel_id,
                                video=file_path,
                                caption=f"🎥 {filename}"
                            )
                        elif file_type == 'animation':  # GIFs
                            await app.send_animation(
                                chat_id=channel_id,
                                animation=file_path,
                                caption=f"🎬 {filename}"
                            )
                        elif file_type == 'audio':
                            await app.send_audio(
                                chat_id=channel_id,
                                audio=file_path,
                                caption=f"🎵 {filename}"
                            )
                        elif file_type == 'voice':
                            await app.send_voice(
                                chat_id=channel_id,
                                voice=file_path,
                                caption=f"🎤 {filename}"
                            )
                        else:  # document for everything else
                            await app.send_document(
                                chat_id=channel_id,
                                document=file_path,
                                caption=f"📄 {filename}"
                            )
                        
                        print(f"✅ Sent {filename} as {file_type} to {channel_id}")
                        files_sent += 1
                        success = True
                        break  # Success, exit retry loop
                        
                    except FloodWait as e:
                        retry_count += 1
                        wait_time = e.value + (2 ** (retry_count - 1))  # Exponential backoff: base wait + 2^(retry-1)
                        jitter = random.uniform(0.5, 1.5)  # Random jitter to avoid thundering herd
                        wait_time = int(wait_time * jitter)
                        print(f"⏳ Flood wait for {filename} (attempt {retry_count}/{max_retries}): Waiting {wait_time} seconds.")
                        await asyncio.sleep(wait_time)
                        if retry_count > max_retries:
                            print(f"❌ Max retries exceeded for {filename}. Skipping.")
                            files_skipped_error += 1
                            break
                    except RPCError as e:
                        print(f"❌ Telegram error sending {filename} as {file_type}: {e}")
                        files_skipped_error += 1
                        break  # No retry for other RPC errors
                    except Exception as e:
                        print(f"❌ Unexpected error with {filename}: {e}")
                        files_skipped_error += 1
                        break  # No retry for unexpected errors
                
                # Proactive delay between sends to avoid triggering flood waits
                if delay_between_sends > 0 and success:
                    await asyncio.sleep(delay_between_sends)
                
                # Update progress bar
                pbar.update(1)
                pbar.set_postfix({
                    "Sent": files_sent, 
                    "Errors": files_skipped_error,
                    "Current": file_type
                })
    
    print(f"\n📊 Summary: {files_sent} files sent, {files_skipped_error} skipped due to errors, {files_skipped_size} skipped due to size.")
    
    # Final file type statistics
    print("\n📁 Final file type breakdown:")
    sent_by_type = {}
    for file_path, filename, file_type in valid_files:
        if files_sent > 0:  # Only count successfully sent files
            sent_by_type[file_type] = sent_by_type.get(file_type, 0) + 1
    
    for file_type, count in sent_by_type.items():
        print(f"  {file_type.capitalize()}: {count} files")

# Example usage
if __name__ == "__main__":
    # Replace with your values
    FOLDER_PATH = "./downloads"  # e.g., "./my_files"
    API_ID = 21347898                        # Your Telegram API ID (integer)
    API_HASH = "98caf2e4f0c25e142c3cbb2e36e683ef"       # Your Telegram API Hash (string)
    BOT_TOKEN = "7944713082:AAFMtxhwah97c6twmGLcWWuTC2wkPerY3tg"     # Get from @BotFather
    CHANNEL_ID = -1002965517245         # or numeric ID like -1001234567890
    DELAY_BETWEEN_SENDS = 0.5             # Seconds between sends (adjust as needed, e.g., 2.0 for slower)
    MAX_RETRIES = 3                       # Max retry attempts per file
    
    asyncio.run(send_files_to_telegram(FOLDER_PATH, BOT_TOKEN, CHANNEL_ID, DELAY_BETWEEN_SENDS, MAX_RETRIES))
