import os
import tempfile
import gradio as gr
from PyPDF2 import PdfReader, PdfWriter
import shutil
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import threading
import time
from typing import List, Tuple, Optional
import logging
import zipfile
from datetime import datetime
from logging.handlers import RotatingFileHandler

# 获取当前脚本所在目录
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# 创建logs目录（在app.py同目录下）
logs_dir = os.path.join(SCRIPT_DIR, "logs")
os.makedirs(logs_dir, exist_ok=True)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        # 控制台输出
        logging.StreamHandler(),
        # 文件输出，每个文件最大10MB，保留5个旧文件
        RotatingFileHandler(
            os.path.join(logs_dir, "pdf_unlock.log"),
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger(__name__)

# 全局配置
MAX_FILE_SIZE_MB = 100  # 最大文件大小限制（MB）
MAX_CONCURRENT_TASKS = 10  # 最大并发处理数
TEMP_FILES_TTL = 3600  # 临时文件保留时间（秒）
SUPPORTED_EXTENSIONS = {".pdf"}  # 支持的文件扩展名

# 全局临时文件记录
temp_files = {}
temp_files_lock = threading.Lock()


def cleanup_temp_files():
    """定期清理临时文件"""
    while True:
        current_time = time.time()
        with temp_files_lock:
            expired_files = [
                (path, created_time)
                for path, created_time in temp_files.items()
                if current_time - created_time > TEMP_FILES_TTL
            ]
            for path, _ in expired_files:
                try:
                    if os.path.exists(path):
                        os.remove(path)
                    temp_files.pop(path, None)
                    logger.info(f"已清理临时文件: {path}")
                except Exception as e:
                    logger.error(f"清理临时文件失败: {path}, 错误: {str(e)}")
        time.sleep(300)  # 每5分钟检查一次


def validate_file(file_obj) -> Tuple[bool, str]:
    """验证文件是否合法"""
    try:
        if not hasattr(file_obj, "name"):
            return False, "无效的文件对象"

        filename = file_obj.name
        filesize = os.path.getsize(filename)

        # 检查文件扩展名
        ext = os.path.splitext(filename)[1].lower()
        if ext not in SUPPORTED_EXTENSIONS:
            return False, f"不支持的文件类型: {ext}"

        # 检查文件大小
        if filesize == 0:
            return False, "文件为空"

        if filesize > MAX_FILE_SIZE_MB * 1024 * 1024:
            return False, f"文件大小超过限制 ({MAX_FILE_SIZE_MB}MB)"

        return True, "验证通过"
    except Exception as e:
        return False, f"文件验证失败: {str(e)}"


def remove_pdf_restrictions(file_obj, progress=None) -> Tuple[Optional[str], str]:
    """移除PDF文件的限制并返回处理后的文件路径"""
    temp_dir = None
    try:
        # 创建临时文件夹
        temp_dir = tempfile.mkdtemp()

        # 保存上传的文件
        original_filename = file_obj.name
        filename = os.path.basename(original_filename)
        output_path = os.path.join(temp_dir, f"{filename}_unlocked")

        # 读取PDF文件
        reader = PdfReader(file_obj)
        writer = PdfWriter()

        total_pages = len(reader.pages)
        for i, page in enumerate(reader.pages):
            writer.add_page(page)
            if progress:
                progress((i + 1) / total_pages)

        # 保存解锁后的PDF
        with open(output_path, "wb") as output_pdf:
            writer.write(output_pdf)

        # 记录临时文件
        with temp_files_lock:
            temp_files[output_path] = time.time()

        return output_path, f"✅ 成功解锁文件: {filename}"
    except Exception as e:
        error_msg = f"❌ 处理失败: {str(e)}"
        logger.error(f"处理文件 {getattr(file_obj, 'name', '未知')} 失败: {str(e)}")
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        return None, error_msg


def process_multiple_files(files, progress=gr.Progress()) -> List[str]:
    """并发处理多个PDF文件"""
    if not files:
        return []

    results = []
    success_files = []
    success_count = 0
    fail_count = 0

    # 创建线程池
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_TASKS) as executor:
        future_to_file = {}

        # 提交所有任务
        for file in files:
            # 首先验证文件
            is_valid, msg = validate_file(file)
            if not is_valid:
                logger.warning(f"❌ {os.path.basename(file.name)}: {msg}")
                fail_count += 1
                continue

            future = executor.submit(remove_pdf_restrictions, file)
            future_to_file[future] = file.name

        # 获取任务结果
        total_tasks = len(future_to_file)
        completed_tasks = 0

        for future in concurrent.futures.as_completed(future_to_file):
            filename = future_to_file[future]
            try:
                output_path, message = future.result()
                if output_path:
                    success_count += 1
                    success_files.append(output_path)
                    logger.info(message)
                else:
                    fail_count += 1
            except Exception as e:
                fail_count += 1
                logger.error(f"❌ 处理文件 {filename} 时发生错误: {str(e)}")

            completed_tasks += 1
            progress((completed_tasks / total_tasks))

    logger.info(
        f"处理完成！成功解锁 {success_count} 个文件，失败 {fail_count} 个文件。"
    )
    return success_files


def create_zip_file(files: List[str]) -> Tuple[Optional[str], str]:
    """将多个PDF文件打包成ZIP"""
    if not files:
        return None, "没有可下载的文件"

    try:
        # 创建临时ZIP文件
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        zip_filename = f"{timestamp}_unlocked_pdfs.zip"
        zip_path = os.path.join(tempfile.gettempdir(), zip_filename)

        # 创建ZIP文件
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for file in files:
                if os.path.exists(file):
                    # 使用原始文件名（去掉路径）
                    arcname = os.path.basename(file)
                    zipf.write(file, arcname)

        # 记录临时文件
        with temp_files_lock:
            temp_files[zip_path] = time.time()

        return zip_path, f"✅ 已创建ZIP文件: {zip_filename}"
    except Exception as e:
        error_msg = f"❌ 创建ZIP文件失败: {str(e)}"
        logger.error(error_msg)
        return None, error_msg


def create_and_download_zip(files):
    """创建并下载ZIP文件"""
    if not files:
        return None
    zip_path, _ = create_zip_file(files)
    return zip_path if zip_path else None


# 启动临时文件清理线程
cleanup_thread = threading.Thread(target=cleanup_temp_files, daemon=True)
cleanup_thread.start()

# 创建Gradio界面
with gr.Blocks(
    title="PDF解锁工具",
    theme=gr.themes.Soft(
        primary_hue="blue",
        secondary_hue="slate",
    ),
    css="""
        .footer {display: none !important}
        .container {max-width: 1000px; margin: auto;}
        .gr-button {min-width: 160px;}
        .warning {
            background-color: #fff3cd;
            color: #856404;
            padding: 1rem;
            border-radius: 8px;
            margin: 1rem 0;
        }
        .info {
            background-color: #e8f4fd;
            color: #004085;
            padding: 1rem;
            border-radius: 8px;
            margin: 1rem 0;
        }
        .download-row {
            display: flex;
            gap: 1rem;
            align-items: center;
            margin-top: 0.5rem;
        }
        """,
) as demo:
    gr.Markdown(
        """
        <div style="text-align: center; margin-bottom: 2rem">

        # 📄 PDF 解锁工具

        ### 轻松移除 PDF 文件的各种使用限制
        </div>
        """
    )

    with gr.Row():
        with gr.Column(scale=1):
            files_input = gr.File(
                label="📁 选择 PDF 文件（支持多选）",
                file_types=[".pdf"],
                file_count="multiple",
            )

            with gr.Row():
                process_btn = gr.Button("🚀 开始解锁", variant="primary", size="lg")
                download_zip_btn = gr.Button(
                    "📦 打包下载", variant="primary", size="lg", interactive=False
                )

        with gr.Column(scale=1):
            output_files = gr.Files(
                label="📥 解锁后的文件（点击单个文件下载）",
                interactive=False,
                type="filepath",
                visible=False,
            )
            zip_download = gr.File(label="ZIP下载", visible=False, interactive=False)
            status_output = gr.Textbox(
                label="💬 处理状态", interactive=False, visible=False
            )

    with gr.Row():
        with gr.Column(scale=1):
            gr.Markdown(
                """
                ## 🔍 功能介绍
                
                本工具可以帮助您移除 PDF 文件中的以下限制：
                - 📝 文档编辑限制
                - 🖨️ 打印限制
                - 📋 复制内容限制
                - ✏️ 注释添加限制
                """
            )

        with gr.Column(scale=1):
            gr.Markdown(
                """
                ## 📝 使用步骤
                
                1. 点击"选择 PDF 文件"上传一个或多个需要解锁的 PDF 文件
                2. 点击"开始解锁"按钮，等待处理完成（支持批量处理）
                3. 在右侧结果区域查看处理进度和状态
                4. 选择下载方式：
                   - 点击单个文件进行下载
                   - 点击"打包下载"按钮将所有文件打包成ZIP下载
                """
            )

    gr.Markdown(
        """
        ## ⚠️ 使用须知
        <div class="warning">
        - 单个文件大小限制：{MAX_FILE_SIZE_MB}MB
        - 支持批量处理，最大并发数：{MAX_CONCURRENT_TASKS}
        - 本工具仅支持未加密的 PDF 文件（无法处理需要密码才能打开的文件）
        </div>
        
        <div class="info">
        💡 为了更好的性能，建议：
        - 确保上传的是有效的 PDF 格式文件
        - 文件大小不要超过限制
        - 批量处理时建议一次上传不超过10个文件
        </div>
        """.format(
            MAX_FILE_SIZE_MB=MAX_FILE_SIZE_MB,
            MAX_CONCURRENT_TASKS=MAX_CONCURRENT_TASKS,
        )
    )

    def process_and_update(files):
        """处理文件并更新界面状态"""
        if not files:
            return {
                output_files: gr.update(value=None, visible=False),
                download_zip_btn: gr.update(interactive=False),
                zip_download: gr.update(value=None),
                status_output: gr.update(value="请选择要处理的PDF文件", visible=True),
            }

        result_files = process_multiple_files(files)
        success_count = len(result_files)
        total_count = len(files)
        fail_count = total_count - success_count

        # 生成状态消息
        if success_count == 0:
            status = "❌ 处理失败：所有文件处理失败，请检查文件是否符合要求。"
        elif fail_count == 0:
            status = f"✅ 处理成功：全部 {success_count} 个文件已解锁完成！"
        else:
            status = (
                f"⚠️ 部分成功：{success_count} 个文件解锁成功，{fail_count} 个失败。"
            )

        return {
            output_files: gr.update(value=result_files, visible=bool(result_files)),
            download_zip_btn: gr.update(interactive=bool(result_files)),
            zip_download: gr.update(value=None),
            status_output: gr.update(value=status, visible=True),
        }

    def download_zip(files):
        """创建并下载ZIP文件"""
        if not files:
            return [
                gr.update(value=None, visible=False),
                gr.update(value="请先处理文件", visible=True),
            ]
        zip_path, message = create_zip_file(files)
        if zip_path:
            return [
                gr.update(value=zip_path, visible=True),
                gr.update(value="✅ ZIP文件已准备就绪，请点击下载", visible=True),
            ]
        return [
            gr.update(value=None, visible=False),
            gr.update(value="❌ ZIP文件创建失败，请重试", visible=True),
        ]

    # 处理按钮点击事件
    process_btn.click(
        fn=process_and_update,
        inputs=[files_input],
        outputs=[output_files, download_zip_btn, zip_download, status_output],
    )

    # 下载按钮点击事件
    download_zip_btn.click(
        fn=download_zip,
        inputs=[output_files],
        outputs=[zip_download, status_output],
    )

    # 文件输入变化事件
    files_input.change(
        fn=lambda: {
            status_output: gr.update(
                value='已选择文件，点击"开始解锁"按钮开始处理', visible=True
            ),
            output_files: gr.update(value=None, visible=False),
            download_zip_btn: gr.update(interactive=False),
            zip_download: gr.update(value=None, visible=False),
        },
        inputs=[],
        outputs=[status_output, output_files, download_zip_btn, zip_download],
    )

# 启动应用
if __name__ == "__main__":
    demo.launch(
        server_name="127.0.0.1",
        server_port=7861,
        show_error=True,
        share=False,
        show_api=False,  # 隐藏 API 使用标志
    )
