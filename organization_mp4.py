import os
import re
import shutil
import tkinter as tk
from tkinter import filedialog, messagebox, ttk, scrolledtext
import cv2
from PIL import Image, ImageTk
from pathlib import Path
from collections import defaultdict
import threading
import math

# ─────────────────────────────────────────────
# 1. VideoInfo: 개별 영상 메타데이터
# ─────────────────────────────────────────────
class VideoInfo:
    def __init__(self, filepath):
        self.filepath = filepath
        self.filename = os.path.basename(filepath)
        self.folder = os.path.basename(os.path.dirname(filepath))
        self.duration = 0
        self.frame_count = 0
        self.fps = 0
        self.width = 0
        self.height = 0
        self.error = None
        self._extract_info()

    def _extract_info(self):
        try:
            cap = cv2.VideoCapture(self.filepath)
            if not cap.isOpened():
                self.error = "파일을 열 수 없음"
                return
            self.fps = cap.get(cv2.CAP_PROP_FPS)
            self.frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            self.width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
            self.height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
            if self.fps > 0:
                self.duration = self.frame_count / self.fps
            cap.release()
        except Exception as e:
            self.error = str(e)

    def to_dict(self):
        return {
            'filename': self.filename,
            'folder': self.folder,
            'filepath': self.filepath,
            'duration': round(self.duration, 3),
            'frame_count': self.frame_count,
            'fps': round(self.fps, 2),
            'resolution': f"{self.width}x{self.height}",
            'width': self.width,
            'height': self.height,
            'error': self.error
        }


# ─────────────────────────────────────────────
# 2. Natural Sort 유틸리티
# ─────────────────────────────────────────────
def natural_sort_key(s):
    return [int(t) if t.isdigit() else t.lower() for t in re.split(r'(\d+)', s)]


# ─────────────────────────────────────────────
# 3. SetMatcher: 세트 매칭 엔진
# ─────────────────────────────────────────────
class SetMatcher:
    def __init__(self, duration_tolerance=0.5):
        self.duration_tolerance = duration_tolerance
        self.cam_folders = {}       # {cam_name: [VideoInfo, ...]}
        self.matched_sets = []      # [(set_name, {cam: VideoInfo, ...}), ...]
        self.unmatched = {}         # {cam: [VideoInfo, ...]}

    def scan_cam_folders(self, root_folder, progress_callback=None):
        self.cam_folders.clear()
        subfolders = sorted([
            d for d in os.listdir(root_folder)
            if os.path.isdir(os.path.join(root_folder, d))
        ], key=natural_sort_key)

        all_files = []
        for cam_name in subfolders:
            cam_path = os.path.join(root_folder, cam_name)
            mp4s = sorted([
                f for f in os.listdir(cam_path)
                if f.lower().endswith('.mp4')
            ], key=natural_sort_key)

            if not mp4s:
                continue

            videos = []
            for f in mp4s:
                fp = os.path.join(cam_path, f)
                all_files.append((cam_name, fp))

        total = len(all_files)
        cam_videos = defaultdict(list)

        for idx, (cam_name, fp) in enumerate(all_files):
            if progress_callback:
                progress_callback(idx + 1, total, f"스캔 중: {os.path.basename(fp)}")
            vi = VideoInfo(fp)
            if vi.error is None:
                cam_videos[cam_name].append(vi)

        self.cam_folders = dict(cam_videos)
        return self.cam_folders

    def match_sets(self):
        self.matched_sets.clear()
        self.unmatched = {cam: [] for cam in self.cam_folders}

        if not self.cam_folders:
            return self.matched_sets

        cam_names = sorted(self.cam_folders.keys(), key=natural_sort_key)

        # 기준 카메라 = 파일 수가 가장 적은 카메라
        ref_cam = min(cam_names, key=lambda c: len(self.cam_folders[c]))
        ref_videos = list(self.cam_folders[ref_cam])

        # 각 카메라의 인덱스 포인터
        pointers = {cam: 0 for cam in cam_names}
        set_index = 1

        for ref_idx, ref_video in enumerate(ref_videos):
            pointers[ref_cam] = ref_idx
            ref_dur = ref_video.duration

            current_set = {ref_cam: ref_video}
            all_match = True

            for cam in cam_names:
                if cam == ref_cam:
                    continue

                cam_list = self.cam_folders[cam]
                ptr = pointers[cam]
                found = False

                # 현재 포인터 위치부터 최대 3개까지 탐색
                search_range = min(ptr + 4, len(cam_list))
                best_match_idx = -1
                best_diff = float('inf')

                for search_idx in range(ptr, search_range):
                    diff = abs(cam_list[search_idx].duration - ref_dur)
                    if diff < best_diff:
                        best_diff = diff
                        best_match_idx = search_idx

                if best_match_idx >= 0 and best_diff <= self.duration_tolerance:
                    # 건너뛴 파일들은 미매칭
                    for skip_idx in range(ptr, best_match_idx):
                        self.unmatched[cam].append(cam_list[skip_idx])
                    current_set[cam] = cam_list[best_match_idx]
                    pointers[cam] = best_match_idx + 1
                    found = True

                if not found:
                    all_match = False

            if all_match and len(current_set) == len(cam_names):
                set_name = f"C{set_index:04d}"
                self.matched_sets.append((set_name, current_set))
                set_index += 1
            elif len(current_set) > 1:
                set_name = f"C{set_index:04d}"
                self.matched_sets.append((set_name, current_set))
                set_index += 1
                # 매칭 안 된 카메라의 포인터 처리
                for cam in cam_names:
                    if cam not in current_set and cam != ref_cam:
                        pass  # 포인터 유지
            else:
                self.unmatched[ref_cam].append(ref_video)

        # 각 카메라에서 남은 파일 미매칭 처리
        for cam in cam_names:
            cam_list = self.cam_folders[cam]
            ptr = pointers[cam] if cam != ref_cam else len(ref_videos)
            if cam == ref_cam:
                continue
            for remaining_idx in range(pointers[cam], len(cam_list)):
                self.unmatched[cam].append(cam_list[remaining_idx])

        return self.matched_sets

    def get_rename_plan(self):
        plan = []
        for set_name, cam_dict in self.matched_sets:
            for cam, video in cam_dict.items():
                old_path = video.filepath
                new_filename = set_name + ".mp4"
                new_path = os.path.join(os.path.dirname(old_path), new_filename)
                plan.append({
                    'set_name': set_name,
                    'cam': cam,
                    'old_path': old_path,
                    'old_filename': video.filename,
                    'new_filename': new_filename,
                    'new_path': new_path,
                    'duration': video.duration,
                    'already_correct': (video.filename == new_filename)
                })
        return plan

    def execute_rename(self, plan, progress_callback=None):
        success = 0
        errors = []
        total = len(plan)

        # 충돌 방지: 임시 이름으로 먼저 변경
        temp_plans = []
        for idx, item in enumerate(plan):
            if item['already_correct']:
                temp_plans.append(None)
                success += 1
                continue
            temp_name = f"__temp_rename_{idx}__.mp4"
            temp_path = os.path.join(os.path.dirname(item['old_path']), temp_name)
            temp_plans.append(temp_path)

        # 1차: 원본 → 임시
        for idx, item in enumerate(plan):
            if progress_callback:
                progress_callback(idx + 1, total * 2, f"임시 이름 변경: {item['old_filename']}")
            if temp_plans[idx] is None:
                continue
            try:
                os.rename(item['old_path'], temp_plans[idx])
            except Exception as e:
                errors.append(f"{item['old_filename']}: {e}")
                temp_plans[idx] = None

        # 2차: 임시 → 최종
        for idx, item in enumerate(plan):
            if progress_callback:
                progress_callback(total + idx + 1, total * 2, f"최종 이름 변경: {item['new_filename']}")
            if temp_plans[idx] is None:
                continue
            try:
                os.rename(temp_plans[idx], item['new_path'])
                success += 1
            except Exception as e:
                errors.append(f"{item['new_filename']}: {e}")
                # 복구 시도
                try:
                    os.rename(temp_plans[idx], item['old_path'])
                except:
                    pass

        return success, errors


# ─────────────────────────────────────────────
# 4. VideoOrganizer: 검증 및 정리
# ─────────────────────────────────────────────
class VideoOrganizer:
    def __init__(self):
        self.root_folder = ""
        self.video_groups = defaultdict(list)
        self.validation_results = {}
        self.duration_tolerance = 0.5
        self.frame_tolerance = 5

    def scan_videos(self, root_folder):
        self.root_folder = root_folder
        self.video_groups.clear()
        video_files = []
        for folder_path, _, files in os.walk(root_folder):
            for file in files:
                if file.lower().endswith('.mp4'):
                    if re.match(r'^C\d{4}\.mp4$', file, re.IGNORECASE):
                        video_files.append(os.path.join(folder_path, file))
        return video_files

    def analyze_videos(self, video_files, progress_callback=None):
        total = len(video_files)
        for i, filepath in enumerate(video_files):
            if progress_callback:
                progress_callback(i + 1, total, f"분석 중: {os.path.basename(filepath)}")
            video_info = VideoInfo(filepath)
            base_name = os.path.splitext(video_info.filename)[0]
            self.video_groups[base_name].append(video_info)

    def validate_groups(self):
        self.validation_results = {}
        for group_name, videos in self.video_groups.items():
            if len(videos) < 2:
                self.validation_results[group_name] = {
                    'status': 'WARNING',
                    'message': '카메라가 1대뿐입니다',
                    'details': []
                }
                continue

            issues = []
            ref_video = videos[0]

            duration_issues = []
            for video in videos[1:]:
                diff = abs(video.duration - ref_video.duration)
                if diff > self.duration_tolerance:
                    duration_issues.append(
                        f"{video.folder}: {diff:.2f}초 차이 "
                        f"(기준: {ref_video.duration:.2f}초, 현재: {video.duration:.2f}초)")
            if duration_issues:
                issues.append(f"영상 길이 불일치:\n  " + "\n  ".join(duration_issues))

            frame_issues = []
            for video in videos[1:]:
                diff = abs(video.frame_count - ref_video.frame_count)
                if diff > self.frame_tolerance:
                    frame_issues.append(
                        f"{video.folder}: {diff}프레임 차이 "
                        f"(기준: {ref_video.frame_count}, 현재: {video.frame_count})")
            if frame_issues:
                issues.append(f"프레임 수 불일치:\n  " + "\n  ".join(frame_issues))

            resolution_issues = []
            for video in videos[1:]:
                if video.width != ref_video.width or video.height != ref_video.height:
                    resolution_issues.append(
                        f"{video.folder}: {video.width}x{video.height} "
                        f"(기준: {ref_video.width}x{ref_video.height})")
            if resolution_issues:
                issues.append(f"해상도 불일치:\n  " + "\n  ".join(resolution_issues))

            fps_issues = []
            for video in videos[1:]:
                if abs(video.fps - ref_video.fps) > 0.1:
                    fps_issues.append(
                        f"{video.folder}: {video.fps:.2f} fps "
                        f"(기준: {ref_video.fps:.2f} fps)")
            if fps_issues:
                issues.append(f"FPS 불일치:\n  " + "\n  ".join(fps_issues))

            if issues:
                self.validation_results[group_name] = {
                    'status': 'ERROR', 'message': '동기화 문제 발견', 'details': issues}
            else:
                self.validation_results[group_name] = {
                    'status': 'OK', 'message': '모든 카메라 동기화 확인', 'details': []}
        return self.validation_results

    def organize_files(self, output_folder="OrganizedVideos", copy_mode=True, progress_callback=None):
        organized_count = 0
        error_count = 0
        errors = []
        total_files = sum(len(v) for v in self.video_groups.values())
        current = 0
        output_path = os.path.join(self.root_folder, output_folder)

        for group_name, videos in self.video_groups.items():
            for video in videos:
                current += 1
                if progress_callback:
                    progress_callback(current, total_files, f"처리 중: {video.filename}")
                try:
                    new_folder = os.path.join(output_path, group_name, video.folder)
                    os.makedirs(new_folder, exist_ok=True)
                    new_filepath = os.path.join(new_folder, video.filename)
                    if copy_mode:
                        shutil.copy2(video.filepath, new_filepath)
                    else:
                        shutil.move(video.filepath, new_filepath)
                    organized_count += 1
                except Exception as e:
                    error_count += 1
                    errors.append(f"{video.filename}: {str(e)}")
        return organized_count, error_count, errors


# ─────────────────────────────────────────────
# 5. SetGridViewer: 세트 그리드 프리뷰어
# ─────────────────────────────────────────────
class SetGridViewer(tk.Toplevel):
    def __init__(self, parent, matched_sets):
        super().__init__(parent)
        self.title("세트 프리뷰어 - 프레임 비교")
        self.geometry("1200x800")
        self.matched_sets = matched_sets
        self.current_set_idx = 0
        self.current_frame = 0
        self.captures = {}
        self.photo_images = []
        self.playing = False
        self.play_speed = 33  # ms (~30fps)

        self._setup_ui()
        self._load_set(0)

        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.bind('<Left>', lambda e: self._prev_frame())
        self.bind('<Right>', lambda e: self._next_frame())
        self.bind('<space>', lambda e: self._toggle_play())
        self.bind('<Up>', lambda e: self._prev_set())
        self.bind('<Down>', lambda e: self._next_set())

    def _setup_ui(self):
        # 상단 컨트롤
        top = tk.Frame(self, bg='#333')
        top.pack(fill=tk.X)

        tk.Button(top, text="◀◀ 이전 세트", command=self._prev_set,
                  bg='#555', fg='white', width=12).pack(side=tk.LEFT, padx=5, pady=5)
        self.set_label = tk.Label(top, text="", bg='#333', fg='white',
                                  font=('Arial', 14, 'bold'))
        self.set_label.pack(side=tk.LEFT, expand=True)
        tk.Button(top, text="다음 세트 ▶▶", command=self._next_set,
                  bg='#555', fg='white', width=12).pack(side=tk.RIGHT, padx=5, pady=5)

        # 영상 그리드
        self.grid_frame = tk.Frame(self, bg='black')
        self.grid_frame.pack(fill=tk.BOTH, expand=True)

        # 하단 컨트롤
        bottom = tk.Frame(self, bg='#333')
        bottom.pack(fill=tk.X)

        ctrl = tk.Frame(bottom, bg='#333')
        ctrl.pack(pady=5)

        tk.Button(ctrl, text="⏮", command=self._first_frame,
                  width=4, bg='#555', fg='white').pack(side=tk.LEFT, padx=2)
        tk.Button(ctrl, text="◀", command=self._prev_frame,
                  width=4, bg='#555', fg='white').pack(side=tk.LEFT, padx=2)
        self.play_btn = tk.Button(ctrl, text="▶", command=self._toggle_play,
                                  width=6, bg='#4CAF50', fg='white')
        self.play_btn.pack(side=tk.LEFT, padx=2)
        tk.Button(ctrl, text="▶", command=self._next_frame,
                  width=4, bg='#555', fg='white').pack(side=tk.LEFT, padx=2)
        tk.Button(ctrl, text="⏭", command=self._last_frame,
                  width=4, bg='#555', fg='white').pack(side=tk.LEFT, padx=2)

        # 프레임 슬라이더
        slider_frame = tk.Frame(bottom, bg='#333')
        slider_frame.pack(fill=tk.X, padx=10, pady=5)

        self.frame_slider = tk.Scale(slider_frame, from_=0, to=100,
                                     orient=tk.HORIZONTAL, bg='#333', fg='white',
                                     highlightthickness=0, command=self._on_slider)
        self.frame_slider.pack(fill=tk.X)

        self.frame_label = tk.Label(bottom, text="Frame: 0/0", bg='#333', fg='white')
        self.frame_label.pack(pady=2)

        # 속도 조절
        speed_frame = tk.Frame(bottom, bg='#333')
        speed_frame.pack(pady=3)
        tk.Label(speed_frame, text="속도:", bg='#333', fg='white').pack(side=tk.LEFT)
        for label, ms in [("0.25x", 132), ("0.5x", 66), ("1x", 33), ("2x", 17)]:
            tk.Button(speed_frame, text=label, width=5, bg='#555', fg='white',
                      command=lambda m=ms: self._set_speed(m)).pack(side=tk.LEFT, padx=2)

    def _release_captures(self):
        for cap in self.captures.values():
            if cap is not None:
                cap.release()
        self.captures.clear()

    def _load_set(self, idx):
        self.playing = False
        self.play_btn.config(text="▶")
        self._release_captures()

        if idx < 0 or idx >= len(self.matched_sets):
            return

        self.current_set_idx = idx
        self.current_frame = 0
        set_name, cam_dict = self.matched_sets[idx]

        self.set_label.config(
            text=f"{set_name}  ({idx + 1}/{len(self.matched_sets)})")

        # 캡처 열기
        self.cam_names = sorted(cam_dict.keys(), key=natural_sort_key)
        max_frames = 0
        for cam in self.cam_names:
            video = cam_dict[cam]
            cap = cv2.VideoCapture(video.filepath)
            self.captures[cam] = cap
            fc = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            if fc > max_frames:
                max_frames = fc

        self.max_frames = max(max_frames - 1, 0)
        self.frame_slider.config(to=self.max_frames)
        self.frame_slider.set(0)

        # 그리드 레이아웃 계산
        for w in self.grid_frame.winfo_children():
            w.destroy()

        n = len(self.cam_names)
        cols = min(n, 3)
        rows = math.ceil(n / cols)

        self.grid_labels = {}
        self.grid_name_labels = {}

        for i, cam in enumerate(self.cam_names):
            r, c = divmod(i, cols)
            cell = tk.Frame(self.grid_frame, bg='black', bd=1, relief=tk.SOLID)
            cell.grid(row=r * 2, column=c, rowspan=2, sticky='nsew', padx=2, pady=2)

            video = cam_dict[cam]
            name_lbl = tk.Label(cell, text=f"{cam}: {video.filename} ({video.duration:.2f}s)",
                                bg='#222', fg='white', font=('Arial', 9))
            name_lbl.pack(fill=tk.X)
            self.grid_name_labels[cam] = name_lbl

            img_lbl = tk.Label(cell, bg='black')
            img_lbl.pack(fill=tk.BOTH, expand=True)
            self.grid_labels[cam] = img_lbl

        for c in range(cols):
            self.grid_frame.columnconfigure(c, weight=1)
        for r in range(rows * 2):
            self.grid_frame.rowconfigure(r, weight=1)

        self._show_frame(0)

    def _show_frame(self, frame_no):
        if not self.captures:
            return
        self.current_frame = max(0, min(frame_no, self.max_frames))
        self.frame_label.config(text=f"Frame: {self.current_frame}/{self.max_frames}")
        self.frame_slider.set(self.current_frame)

        self.photo_images.clear()

        for cam in self.cam_names:
            cap = self.captures.get(cam)
            label = self.grid_labels.get(cam)
            if cap is None or label is None:
                continue

            cap.set(cv2.CAP_PROP_POS_FRAMES, self.current_frame)
            ret, frame = cap.read()

            if ret:
                lw = label.winfo_width()
                lh = label.winfo_height()
                if lw < 10:
                    lw = 380
                if lh < 10:
                    lh = 280

                h, w = frame.shape[:2]
                scale = min(lw / w, lh / h)
                new_w = max(int(w * scale), 1)
                new_h = max(int(h * scale), 1)

                frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                frame_resized = cv2.resize(frame_rgb, (new_w, new_h))
                img = Image.fromarray(frame_resized)
                photo = ImageTk.PhotoImage(img)
                self.photo_images.append(photo)
                label.config(image=photo)
            else:
                label.config(image='', text='END', fg='gray')

    def _on_slider(self, val):
        frame_no = int(float(val))
        if frame_no != self.current_frame:
            self._show_frame(frame_no)

    def _next_frame(self):
        self._show_frame(self.current_frame + 1)

    def _prev_frame(self):
        self._show_frame(self.current_frame - 1)

    def _first_frame(self):
        self._show_frame(0)

    def _last_frame(self):
        self._show_frame(self.max_frames)

    def _toggle_play(self):
        self.playing = not self.playing
        if self.playing:
            self.play_btn.config(text="⏸")
            self._play_loop()
        else:
            self.play_btn.config(text="▶")

    def _play_loop(self):
        if not self.playing:
            return
        if self.current_frame >= self.max_frames:
            self.playing = False
            self.play_btn.config(text="▶")
            return
        self._show_frame(self.current_frame + 1)
        self.after(self.play_speed, self._play_loop)

    def _set_speed(self, ms):
        self.play_speed = ms

    def _prev_set(self):
        if self.current_set_idx > 0:
            self._load_set(self.current_set_idx - 1)

    def _next_set(self):
        if self.current_set_idx < len(self.matched_sets) - 1:
            self._load_set(self.current_set_idx + 1)

    def _on_close(self):
        self.playing = False
        self._release_captures()
        self.destroy()


# ─────────────────────────────────────────────
# 6. RenamePlanDialog: 리네임 미리보기 다이얼로그
# ─────────────────────────────────────────────
class RenamePlanDialog(tk.Toplevel):
    def __init__(self, parent, plan, unmatched):
        super().__init__(parent)
        self.title("리네임 계획 미리보기")
        self.geometry("900x600")
        self.result = False
        self.plan = plan
        self.unmatched = unmatched
        self.transient(parent)
        self.grab_set()

        self._setup_ui()
        self.wait_window(self)

    def _setup_ui(self):
        tk.Label(self, text="파일 리네임 계획", font=('Arial', 14, 'bold')).pack(pady=10)

        # 트리뷰
        tree_frame = tk.Frame(self)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        columns = ('set', 'cam', 'old_name', 'new_name', 'duration')
        self.tree = ttk.Treeview(tree_frame, columns=columns, show='headings', height=20)

        self.tree.heading('set', text='세트')
        self.tree.heading('cam', text='카메라')
        self.tree.heading('old_name', text='현재 파일명')
        self.tree.heading('new_name', text='변경 후 파일명')
        self.tree.heading('duration', text='길이(초)')

        self.tree.column('set', width=80, anchor='center')
        self.tree.column('cam', width=100, anchor='center')
        self.tree.column('old_name', width=300)
        self.tree.column('new_name', width=150, anchor='center')
        self.tree.column('duration', width=100, anchor='center')

        scrollbar = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)

        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # 데이터 삽입
        change_count = 0
        for item in self.plan:
            tag = 'same' if item['already_correct'] else 'change'
            if not item['already_correct']:
                change_count += 1
            self.tree.insert('', tk.END, values=(
                item['set_name'],
                item['cam'],
                item['old_filename'],
                item['new_filename'],
                f"{item['duration']:.2f}"
            ), tags=(tag,))

        self.tree.tag_configure('change', foreground='blue')
        self.tree.tag_configure('same', foreground='gray')

        # 미매칭 파일 표시
        total_unmatched = sum(len(v) for v in self.unmatched.values())
        if total_unmatched > 0:
            unmatch_frame = tk.LabelFrame(self, text=f"미매칭 파일 ({total_unmatched}개)",
                                          fg='red')
            unmatch_frame.pack(fill=tk.X, padx=10, pady=5)

            unmatch_text = scrolledtext.ScrolledText(unmatch_frame, height=4, wrap=tk.WORD)
            unmatch_text.pack(fill=tk.X, padx=5, pady=5)
            for cam, videos in self.unmatched.items():
                for v in videos:
                    unmatch_text.insert(tk.END, f"  {cam}/{v.filename} ({v.duration:.2f}초)\n")
            unmatch_text.config(state=tk.DISABLED)

        # 요약
        summary = tk.Label(self,
                           text=f"총 {len(self.plan)}개 파일 중 {change_count}개 이름 변경 예정",
                           font=('Arial', 11))
        summary.pack(pady=5)

        # 버튼
        btn_frame = tk.Frame(self)
        btn_frame.pack(pady=10)

        tk.Button(btn_frame, text="실행", command=self._confirm,
                  width=15, height=2, bg='#4CAF50', fg='white').pack(side=tk.LEFT, padx=10)
        tk.Button(btn_frame, text="취소", command=self._cancel,
                  width=15, height=2, bg='#f44336', fg='white').pack(side=tk.LEFT, padx=10)

    def _confirm(self):
        self.result = True
        self.destroy()

    def _cancel(self):
        self.result = False
        self.destroy()


# ─────────────────────────────────────────────
# 7. 메인 GUI
# ─────────────────────────────────────────────
class VideoOrganizerGUI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("다중 카메라 영상 정리 프로그램")
        self.root.geometry("1100x750")

        self.organizer = VideoOrganizer()
        self.matcher = SetMatcher()
        self.video_files = []

        self.setup_ui()

    def setup_ui(self):
        # 상단 - 폴더 선택
        top_frame = tk.Frame(self.root, pady=10)
        top_frame.pack(fill=tk.X, padx=10)

        tk.Button(top_frame, text="폴더 선택", command=self.select_folder,
                  width=15, height=2, bg='#2196F3', fg='white').pack(side=tk.LEFT, padx=5)

        self.folder_label = tk.Label(top_frame, text="폴더를 선택하세요", anchor='w')
        self.folder_label.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=10)

        # 설정 프레임
        settings_frame = tk.LabelFrame(self.root, text="설정", pady=10)
        settings_frame.pack(fill=tk.X, padx=10, pady=5)

        tolerance_frame = tk.Frame(settings_frame)
        tolerance_frame.pack(fill=tk.X, padx=10)

        tk.Label(tolerance_frame, text="영상 길이 허용 오차 (초):").pack(side=tk.LEFT, padx=5)
        self.duration_var = tk.DoubleVar(value=0.5)
        tk.Spinbox(tolerance_frame, from_=0.1, to=5.0, increment=0.1,
                   textvariable=self.duration_var, width=10).pack(side=tk.LEFT, padx=5)

        tk.Label(tolerance_frame, text="프레임 수 허용 오차:").pack(side=tk.LEFT, padx=20)
        self.frame_var = tk.IntVar(value=5)
        tk.Spinbox(tolerance_frame, from_=1, to=100, increment=1,
                   textvariable=self.frame_var, width=10).pack(side=tk.LEFT, padx=5)

        mode_frame = tk.Frame(settings_frame)
        mode_frame.pack(fill=tk.X, padx=10, pady=5)

        self.copy_mode_var = tk.BooleanVar(value=True)
        tk.Radiobutton(mode_frame, text="파일 복사", variable=self.copy_mode_var,
                       value=True).pack(side=tk.LEFT, padx=10)
        tk.Radiobutton(mode_frame, text="파일 이동", variable=self.copy_mode_var,
                       value=False).pack(side=tk.LEFT, padx=10)

        # 컨트롤 버튼 - 6단계
        control_frame = tk.Frame(self.root)
        control_frame.pack(fill=tk.X, padx=10, pady=10)

        tk.Button(control_frame, text="1. 스캔 & 매칭", command=self.step1_scan_match,
                  width=15, height=2, bg='#FF5722', fg='white').pack(side=tk.LEFT, padx=3)
        tk.Button(control_frame, text="2. 세트 프리뷰", command=self.step2_preview_sets,
                  width=15, height=2, bg='#E91E63', fg='white').pack(side=tk.LEFT, padx=3)
        tk.Button(control_frame, text="3. 리네임", command=self.step3_rename,
                  width=15, height=2, bg='#9C27B0', fg='white').pack(side=tk.LEFT, padx=3)
        tk.Button(control_frame, text="4. 분석", command=self.step4_analyze,
                  width=15, height=2, bg='#FF9800', fg='white').pack(side=tk.LEFT, padx=3)
        tk.Button(control_frame, text="5. 검증", command=self.step5_validate,
                  width=15, height=2, bg='#2196F3', fg='white').pack(side=tk.LEFT, padx=3)
        tk.Button(control_frame, text="6. 정리 실행", command=self.step6_organize,
                  width=15, height=2, bg='#4CAF50', fg='white').pack(side=tk.LEFT, padx=3)

        # 진행 상황
        self.progress_var = tk.StringVar(value="준비 중...")
        tk.Label(self.root, textvariable=self.progress_var, anchor='w').pack(fill=tk.X, padx=10)

        self.progress_bar = ttk.Progressbar(self.root, mode='determinate')
        self.progress_bar.pack(fill=tk.X, padx=10, pady=5)

        # 결과 표시
        result_frame = tk.LabelFrame(self.root, text="결과")
        result_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        self.result_text = scrolledtext.ScrolledText(result_frame, wrap=tk.WORD, height=20)
        self.result_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        self.result_text.tag_config('header', font=('Arial', 12, 'bold'))
        self.result_text.tag_config('ok', foreground='green')
        self.result_text.tag_config('warning', foreground='orange')
        self.result_text.tag_config('error', foreground='red')
        self.result_text.tag_config('info', foreground='blue')
        self.result_text.tag_config('change', foreground='#9C27B0')

    # ── 폴더 선택 ──
    def select_folder(self):
        folder = filedialog.askdirectory()
        if folder:
            self.folder_label.config(text=folder)
            self.organizer.root_folder = folder

            self.result_text.delete(1.0, tk.END)
            self.result_text.insert(tk.END, f"선택된 폴더: {folder}\n\n", 'header')

            # 하위 폴더 탐색
            subfolders = sorted([
                d for d in os.listdir(folder)
                if os.path.isdir(os.path.join(folder, d))
            ], key=natural_sort_key)

            self.result_text.insert(tk.END, f"발견된 카메라 폴더: {len(subfolders)}개\n", 'info')
            for sf in subfolders:
                sf_path = os.path.join(folder, sf)
                mp4_count = len([f for f in os.listdir(sf_path) if f.lower().endswith('.mp4')])
                self.result_text.insert(tk.END, f"  {sf}: MP4 {mp4_count}개\n")

    def update_progress(self, current, total, message):
        self.progress_var.set(f"{message} ({current}/{total})")
        if total > 0:
            self.progress_bar['value'] = (current / total) * 100
        self.root.update_idletasks()

    # ── Step 1: 스캔 & 매칭 ──
    def step1_scan_match(self):
        folder = self.organizer.root_folder
        if not folder or not os.path.isdir(folder):
            messagebox.showwarning("경고", "먼저 폴더를 선택하세요.")
            return

        self.result_text.delete(1.0, tk.END)
        self.result_text.insert(tk.END, "Step 1: 스캔 & 세트 매칭\n\n", 'header')

        # 허용 오차 업데이트
        self.matcher = SetMatcher(duration_tolerance=self.duration_var.get())

        # 스캔
        self.result_text.insert(tk.END, "카메라 폴더 스캔 중...\n", 'info')
        self.root.update_idletasks()

        cam_folders = self.matcher.scan_cam_folders(folder, self.update_progress)

        if not cam_folders:
            self.result_text.insert(tk.END, "\nMP4 파일이 있는 하위 폴더를 찾지 못했습니다.\n", 'error')
            return

        for cam, videos in sorted(cam_folders.items(), key=lambda x: natural_sort_key(x[0])):
            self.result_text.insert(tk.END, f"\n  {cam}: {len(videos)}개 영상\n", 'info')
            for v in videos:
                self.result_text.insert(tk.END,
                    f"    {v.filename}  |  {v.duration:.2f}초  |  "
                    f"{v.frame_count}f  |  {v.width}x{v.height}  |  {v.fps:.1f}fps\n")

        # 매칭
        self.result_text.insert(tk.END, "\n세트 매칭 중...\n", 'header')
        self.root.update_idletasks()

        matched = self.matcher.match_sets()

        self.result_text.insert(tk.END, f"\n매칭 결과: {len(matched)}개 세트\n\n", 'ok')

        for set_name, cam_dict in matched:
            durations = [v.duration for v in cam_dict.values()]
            max_diff = max(durations) - min(durations) if len(durations) > 1 else 0

            diff_tag = 'ok' if max_diff <= self.duration_var.get() else 'warning'
            self.result_text.insert(tk.END, f"  {set_name}: ", 'header')
            self.result_text.insert(tk.END, f"최대 차이 {max_diff:.3f}초\n", diff_tag)

            for cam in sorted(cam_dict.keys(), key=natural_sort_key):
                v = cam_dict[cam]
                self.result_text.insert(tk.END,
                    f"    {cam}/{v.filename} -> {set_name}.mp4  ({v.duration:.2f}초)\n", 'change')

        # 미매칭 표시
        total_unmatched = sum(len(v) for v in self.matcher.unmatched.values())
        if total_unmatched > 0:
            self.result_text.insert(tk.END, f"\n미매칭 파일: {total_unmatched}개\n", 'warning')
            for cam, videos in self.matcher.unmatched.items():
                for v in videos:
                    self.result_text.insert(tk.END,
                        f"  {cam}/{v.filename} ({v.duration:.2f}초)\n", 'warning')

        self.progress_var.set(f"매칭 완료: {len(matched)}개 세트, 미매칭 {total_unmatched}개")

    # ── Step 2: 세트 프리뷰 ──
    def step2_preview_sets(self):
        if not self.matcher.matched_sets:
            messagebox.showwarning("경고", "먼저 Step 1 (스캔 & 매칭)을 실행하세요.")
            return

        SetGridViewer(self.root, self.matcher.matched_sets)

    # ── Step 3: 리네임 ──
    def step3_rename(self):
        if not self.matcher.matched_sets:
            messagebox.showwarning("경고", "먼저 Step 1 (스캔 & 매칭)을 실행하세요.")
            return

        plan = self.matcher.get_rename_plan()

        if not plan:
            messagebox.showinfo("정보", "변경할 파일이 없습니다.")
            return

        dialog = RenamePlanDialog(self.root, plan, self.matcher.unmatched)

        if dialog.result:
            self.result_text.delete(1.0, tk.END)
            self.result_text.insert(tk.END, "Step 3: 리네임 실행 중...\n\n", 'header')
            self.root.update_idletasks()

            success, errors = self.matcher.execute_rename(plan, self.update_progress)

            self.result_text.insert(tk.END, f"리네임 완료: 성공 {success}개\n", 'ok')
            if errors:
                self.result_text.insert(tk.END, f"오류 {len(errors)}개:\n", 'error')
                for err in errors:
                    self.result_text.insert(tk.END, f"  {err}\n", 'error')

            self.progress_var.set(f"리네임 완료: 성공 {success}개, 오류 {len(errors)}개")
        else:
            self.result_text.insert(tk.END, "\n리네임이 취소되었습니다.\n", 'warning')

    # ── Step 4: 분석 ──
    def step4_analyze(self):
        folder = self.organizer.root_folder
        if not folder or not os.path.isdir(folder):
            messagebox.showwarning("경고", "먼저 폴더를 선택하세요.")
            return

        self.organizer.video_groups.clear()
        self.organizer.validation_results.clear()

        self.video_files = self.organizer.scan_videos(folder)

        if not self.video_files:
            messagebox.showwarning("경고",
                "C0001.mp4 형태의 파일을 찾지 못했습니다.\nStep 3 (리네임)을 먼저 실행하세요.")
            return

        self.result_text.delete(1.0, tk.END)
        self.result_text.insert(tk.END, "Step 4: 비디오 분석\n\n", 'header')

        self.organizer.duration_tolerance = self.duration_var.get()
        self.organizer.frame_tolerance = self.frame_var.get()

        self.organizer.analyze_videos(self.video_files, self.update_progress)

        self.result_text.insert(tk.END,
            f"분석 완료: {len(self.organizer.video_groups)}개 그룹\n\n", 'ok')

        for group_name, videos in sorted(self.organizer.video_groups.items()):
            self.result_text.insert(tk.END, f"  {group_name}: ", 'header')
            self.result_text.insert(tk.END, f"{len(videos)}개 카메라\n")
            for video in videos:
                info = video.to_dict()
                self.result_text.insert(tk.END,
                    f"    {info['folder']}: {info['duration']:.2f}초, "
                    f"{info['frame_count']}프레임, {info['resolution']}, "
                    f"{info['fps']:.1f}fps\n")
            self.result_text.insert(tk.END, "\n")

    # ── Step 5: 검증 ──
    def step5_validate(self):
        if not self.organizer.video_groups:
            messagebox.showwarning("경고", "먼저 Step 4 (분석)를 실행하세요.")
            return

        self.result_text.delete(1.0, tk.END)
        self.result_text.insert(tk.END, "Step 5: 동기화 검증\n\n", 'header')

        validation_results = self.organizer.validate_groups()

        ok_count = sum(1 for v in validation_results.values() if v['status'] == 'OK')
        error_count = sum(1 for v in validation_results.values() if v['status'] == 'ERROR')
        warning_count = sum(1 for v in validation_results.values() if v['status'] == 'WARNING')

        self.result_text.insert(tk.END,
            f"검증 완료: 정상 {ok_count}개, 경고 {warning_count}개, 오류 {error_count}개\n\n",
            'header')

        for group_name, result in sorted(validation_results.items()):
            status = result['status']
            if status == 'OK':
                self.result_text.insert(tk.END,
                    f"  [OK] {group_name}: {result['message']}\n", 'ok')
            elif status == 'WARNING':
                self.result_text.insert(tk.END,
                    f"  [WARNING] {group_name}: {result['message']}\n", 'warning')
            else:
                self.result_text.insert(tk.END,
                    f"  [ERROR] {group_name}: {result['message']}\n", 'error')
                for detail in result['details']:
                    self.result_text.insert(tk.END, f"      {detail}\n", 'error')
            self.result_text.insert(tk.END, "\n")

        if error_count > 0:
            messagebox.showwarning("경고",
                f"{error_count}개 그룹에서 동기화 문제가 발견되었습니다.\n"
                "상세 내용을 확인하세요.")

    # ── Step 6: 정리 실행 ──
    def step6_organize(self):
        if not self.organizer.video_groups:
            messagebox.showwarning("경고", "먼저 Step 4 (분석)를 실행하세요.")
            return

        if not self.organizer.validation_results:
            resp = messagebox.askyesno("확인",
                "검증을 하지 않았습니다. 그래도 진행하시겠습니까?")
            if not resp:
                return

        # ERROR 상태 그룹 경고
        error_groups = [name for name, v in self.organizer.validation_results.items()
                        if v['status'] == 'ERROR']
        if error_groups:
            resp = messagebox.askyesno("경고",
                f"동기화 오류가 있는 그룹 {len(error_groups)}개:\n"
                f"{', '.join(error_groups[:10])}\n\n"
                f"오류 그룹을 제외하고 진행하시겠습니까?\n"
                f"(예: 오류 제외 / 아니요: 전체 취소)")
            if not resp:
                return
            # 오류 그룹 제거
            for name in error_groups:
                if name in self.organizer.video_groups:
                    del self.organizer.video_groups[name]

        mode = "복사" if self.copy_mode_var.get() else "이동"
        resp = messagebox.askyesno("최종 확인",
            f"파일을 {mode}하여 정리합니다.\n"
            f"출력 폴더: OrganizedVideos\n"
            f"대상: {len(self.organizer.video_groups)}개 그룹\n"
            f"진행하시겠습니까?")
        if not resp:
            return

        self.result_text.delete(1.0, tk.END)
        self.result_text.insert(tk.END, f"Step 6: 파일 정리 ({mode} 모드)\n\n", 'header')

        organized, error_count, error_details = self.organizer.organize_files(
            copy_mode=self.copy_mode_var.get(),
            progress_callback=self.update_progress
        )

        self.result_text.insert(tk.END, f"\n정리 완료!\n", 'ok')
        self.result_text.insert(tk.END, f"성공: {organized}개 파일\n", 'info')
        if error_count > 0:
            self.result_text.insert(tk.END, f"실패: {error_count}개 파일\n", 'error')
            for err in error_details:
                self.result_text.insert(tk.END, f"  {err}\n", 'error')

        output_path = os.path.join(self.organizer.root_folder, 'OrganizedVideos')
        self.result_text.insert(tk.END, f"\n출력 폴더: {output_path}\n", 'info')

        messagebox.showinfo("완료",
            f"파일 정리 완료!\n성공: {organized}개, 실패: {error_count}개")

    def run(self):
        self.root.mainloop()


# ─────────────────────────────────────────────
# 진입점
# ─────────────────────────────────────────────
if __name__ == "__main__":
    try:
        import cv2
    except ImportError:
        print("OpenCV가 설치되지 않았습니다.")
        print("설치: pip install opencv-python")
        import sys
        sys.exit(1)

    try:
        from PIL import Image, ImageTk
    except ImportError:
        print("Pillow가 설치되지 않았습니다.")
        print("설치: pip install Pillow")
        import sys
        sys.exit(1)

    app = VideoOrganizerGUI()
    app.run()
