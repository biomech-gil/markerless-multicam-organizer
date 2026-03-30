import os
import re
import json
import shutil
import math
import threading
import subprocess
import tkinter as tk
from tkinter import filedialog, messagebox, ttk, scrolledtext
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime


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
# 2. 정렬 유틸리티
# ─────────────────────────────────────────────
def natural_sort_key(s):
    return [int(t) if t.isdigit() else t.lower() for t in re.split(r'(\d+)', s)]


def zcam_sort_key(filename):
    """ZCAM 파일명에서 CNNNN 시퀀스 번호를 추출하여 정렬 키로 사용.
    예: 'H001C0023_21000221131242_0001.mp4' -> C0023 -> 23
    패턴 불일치 시 natural_sort_key로 폴백.
    """
    m = re.match(r'[A-Za-z]\d{3}C(\d+)', filename)
    if m:
        return (0, int(m.group(1)), filename)
    return (1, 0, natural_sort_key(filename))


# ─────────────────────────────────────────────
# 3. FileHistoryManager: 파일 변경 이력 관리
# ─────────────────────────────────────────────
class FileHistoryManager:
    """파일 변경 이력을 영구 저장하여 최초 원본 상태로 복원을 지원한다.

    .file_history/original_state.json:
        {cam_folder: {현재파일명: {original_filename, duration, ...}}}
        - 키(현재파일명)는 리네임 시 갱신되지만, original_filename은 절대 변경되지 않음

    .file_history/change_log.json:
        [{timestamp, cam, from, to}, ...]  변경 이력 전체 기록
    """

    HISTORY_DIR = ".file_history"
    ORIGINAL_STATE_FILE = "original_state.json"
    CHANGE_LOG_FILE = "change_log.json"

    def __init__(self, root_folder):
        self.root_folder = root_folder
        self.history_dir = os.path.join(root_folder, self.HISTORY_DIR)
        self.original_state_path = os.path.join(self.history_dir, self.ORIGINAL_STATE_FILE)
        self.change_log_path = os.path.join(self.history_dir, self.CHANGE_LOG_FILE)
        self.original_state = {}   # {cam: {current_filename: {original_filename, ...}}}
        self.change_log = []       # [{timestamp, cam, from, to}, ...]
        self._ensure_history_dir()
        self._load()

    def _ensure_history_dir(self):
        os.makedirs(self.history_dir, exist_ok=True)

    def _load(self):
        if os.path.exists(self.original_state_path):
            with open(self.original_state_path, 'r', encoding='utf-8') as f:
                self.original_state = json.load(f)
        if os.path.exists(self.change_log_path):
            with open(self.change_log_path, 'r', encoding='utf-8') as f:
                self.change_log = json.load(f)

    def _save(self):
        with open(self.original_state_path, 'w', encoding='utf-8') as f:
            json.dump(self.original_state, f, ensure_ascii=False, indent=2)
        with open(self.change_log_path, 'w', encoding='utf-8') as f:
            json.dump(self.change_log, f, ensure_ascii=False, indent=2)

    def has_history(self):
        return bool(self.original_state)

    def capture_initial_state(self, cam_folders):
        """최초 스캔 시 원본 상태를 캡처한다. 이미 이력이 있으면 건너뛴다."""
        if self.has_history():
            return False
        for cam_name, videos in cam_folders.items():
            self.original_state[cam_name] = {}
            for video in videos:
                self.original_state[cam_name][video.filename] = {
                    'original_filename': video.filename,
                    'duration': round(video.duration, 3),
                    'frame_count': video.frame_count,
                    'fps': round(video.fps, 2),
                    'resolution': f"{video.width}x{video.height}",
                }
        self._save()
        return True

    def record_renames(self, rename_log):
        """execute_rename이 반환한 rename_log를 받아 이력을 갱신한다."""
        timestamp = datetime.now().isoformat()
        for entry in rename_log:
            cam = entry['cam']
            old = entry['old']
            new = entry['new']
            if old == new:
                continue
            if cam in self.original_state and old in self.original_state[cam]:
                info = self.original_state[cam].pop(old)
                self.original_state[cam][new] = info
                self.change_log.append({
                    'timestamp': timestamp,
                    'cam': cam,
                    'from': old,
                    'to': new,
                })
        self._save()

    def get_restore_plan(self):
        """원본 복원 계획 반환: 현재 파일명 ≠ 원본 파일명인 항목만."""
        plan = []
        for cam_name, files in self.original_state.items():
            cam_dir = os.path.join(self.root_folder, cam_name)
            for current_filename, info in files.items():
                original_filename = info['original_filename']
                if current_filename != original_filename:
                    plan.append({
                        'cam': cam_name,
                        'dir': cam_dir,
                        'current': current_filename,
                        'original': original_filename,
                        'metadata': info,
                    })
        return plan

    def restore_to_original(self, progress_callback=None):
        """모든 파일을 최초 원본 상태로 복원한다."""
        plan = self.get_restore_plan()
        if not plan:
            return 0, ["복원할 파일이 없습니다. (이미 원본 상태)"]

        success = 0
        errors = []
        total = len(plan)

        # 충돌 방지: 현재 → 임시 → 원본
        temp_plans = []
        for idx, item in enumerate(plan):
            temp_name = f"__temp_restore_{idx}__.mp4"
            temp_path = os.path.join(item['dir'], temp_name)
            temp_plans.append(temp_path)

        # 1차: 현재 → 임시
        for idx, item in enumerate(plan):
            if progress_callback:
                progress_callback(idx + 1, total * 2,
                                  f"임시 변경: {item['current']}")
            current_path = os.path.join(item['dir'], item['current'])
            if not os.path.exists(current_path):
                errors.append(f"{item['cam']}/{item['current']}: 파일을 찾을 수 없음")
                temp_plans[idx] = None
                continue
            try:
                os.rename(current_path, temp_plans[idx])
            except Exception as e:
                errors.append(f"{item['current']}: {e}")
                temp_plans[idx] = None

        # 2차: 임시 → 원본
        timestamp = datetime.now().isoformat()
        for idx, item in enumerate(plan):
            if progress_callback:
                progress_callback(total + idx + 1, total * 2,
                                  f"복원: {item['original']}")
            if temp_plans[idx] is None:
                continue
            original_path = os.path.join(item['dir'], item['original'])
            try:
                os.rename(temp_plans[idx], original_path)
                success += 1
                # 매핑 갱신: 키를 원본 파일명으로 되돌림
                cam_state = self.original_state[item['cam']]
                info = cam_state.pop(item['current'])
                cam_state[item['original']] = info
                self.change_log.append({
                    'timestamp': timestamp,
                    'cam': item['cam'],
                    'from': item['current'],
                    'to': item['original'],
                    'action': 'restore',
                })
            except Exception as e:
                errors.append(f"{item['original']}: {e}")
                try:
                    os.rename(temp_plans[idx],
                              os.path.join(item['dir'], item['current']))
                except:
                    pass

        self._save()
        return success, errors

    def get_change_summary(self):
        """변경 이력 요약 반환."""
        changed_count = 0
        total_files = 0
        for files in self.original_state.values():
            for current, info in files.items():
                total_files += 1
                if current != info['original_filename']:
                    changed_count += 1
        return {
            'total_files': total_files,
            'changed_files': changed_count,
            'total_changes': len(self.change_log),
        }


# ─────────────────────────────────────────────
# 4. SetMatcher: 세트 매칭 엔진
# ─────────────────────────────────────────────
class SetMatcher:
    def __init__(self, duration_tolerance=0.5):
        self.duration_tolerance = duration_tolerance
        self.cam_folders = {}       # {cam_name: [VideoInfo, ...]}
        self.matched_sets = []      # [(set_name, {cam: VideoInfo, ...}), ...]
        self.unmatched = {}         # {cam: [VideoInfo, ...]}
        self.calibration_videos = {}  # {cam_name: VideoInfo}

    def set_calibration(self, cal_dict):
        """캘리브레이션 영상 지정. 빈 dict이면 해제."""
        self.calibration_videos = dict(cal_dict)

    def scan_cam_folders(self, root_folder, progress_callback=None):
        self.cam_folders.clear()
        subfolders = sorted([
            d for d in os.listdir(root_folder)
            if os.path.isdir(os.path.join(root_folder, d))
            and d not in (FileHistoryManager.HISTORY_DIR, 'OrganizedVideos')
        ], key=natural_sort_key)

        all_files = []
        for cam_name in subfolders:
            cam_path = os.path.join(root_folder, cam_name)
            mp4s = sorted([
                f for f in os.listdir(cam_path)
                if f.lower().endswith('.mp4')
            ], key=zcam_sort_key)

            if not mp4s:
                continue

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

        # 캘리브레이션 영상 제외한 작업용 목록
        working_folders = {}
        for cam in cam_names:
            if cam in self.calibration_videos:
                cal = self.calibration_videos[cam]
                working_folders[cam] = [v for v in self.cam_folders[cam]
                                        if v.filepath != cal.filepath]
            else:
                working_folders[cam] = list(self.cam_folders[cam])

        # 모든 카메라의 파일 수가 동일한지 확인
        counts = [len(working_folders[c]) for c in cam_names]
        all_same_count = len(set(counts)) == 1 and counts[0] > 0

        if all_same_count:
            # ── 파일 수 동일: CNNNN 순서대로 1:1 매핑 (duration 무시) ──
            n_files = counts[0]
            set_index = 2 if self.calibration_videos else 1
            for i in range(n_files):
                current_set = {}
                for cam in cam_names:
                    current_set[cam] = working_folders[cam][i]
                set_name = f"C{set_index:04d}"
                self.matched_sets.append((set_name, current_set))
                set_index += 1
        else:
            # ── 파일 수 다름: duration 기반 매칭 ──
            ref_cam = min(cam_names, key=lambda c: len(working_folders[c]))
            ref_videos = list(working_folders[ref_cam])

            pointers = {cam: 0 for cam in cam_names}
            set_index = 2 if self.calibration_videos else 1

            for ref_idx, ref_video in enumerate(ref_videos):
                pointers[ref_cam] = ref_idx
                ref_dur = ref_video.duration

                current_set = {ref_cam: ref_video}
                all_match = True

                for cam in cam_names:
                    if cam == ref_cam:
                        continue

                    cam_list = working_folders[cam]
                    ptr = pointers[cam]
                    found = False

                    search_range = min(ptr + 4, len(cam_list))
                    best_match_idx = -1
                    best_diff = float('inf')

                    for search_idx in range(ptr, search_range):
                        diff = abs(cam_list[search_idx].duration - ref_dur)
                        if diff < best_diff:
                            best_diff = diff
                            best_match_idx = search_idx

                    if best_match_idx >= 0 and best_diff <= self.duration_tolerance:
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
                else:
                    self.unmatched[ref_cam].append(ref_video)

            for cam in cam_names:
                if cam == ref_cam:
                    continue
                for remaining_idx in range(pointers[cam], len(working_folders[cam])):
                    self.unmatched[cam].append(working_folders[cam][remaining_idx])

        # 캘리브레이션 세트를 C0001로 맨 앞에 삽입
        if self.calibration_videos:
            self.matched_sets.insert(0, ("C0001", dict(self.calibration_videos)))

        # CNNNN 단조증가 강제 적용
        self._enforce_cnnnn_order()

        return self.matched_sets

    @staticmethod
    def _get_cnnnn(video):
        """파일명에서 CNNNN 시퀀스 번호를 추출한다. 없으면 -1."""
        m = re.match(r'[A-Za-z]\d{3}C(\d+)', video.filename)
        return int(m.group(1)) if m else -1

    def _enforce_cnnnn_order(self):
        """각 카메라에서 세트 순서대로 CNNNN이 단조증가하도록 재배치한다.

        duration 매칭이 CNNNN 순서를 깨뜨린 경우, 카메라별로
        파일을 CNNNN 순서로 재정렬하여 세트에 재배치한다.
        세트 구조(어떤 세트에 어떤 카메라가 포함되는지)는 유지된다.
        """
        # 캘리브레이션 세트(C0001)는 제외
        start_idx = 1 if self.calibration_videos else 0

        cam_names = set()
        for idx in range(start_idx, len(self.matched_sets)):
            cam_names.update(self.matched_sets[idx][1].keys())

        for cam in cam_names:
            # 이 카메라가 포함된 세트 인덱스 + 비디오 수집
            entries = []
            for idx in range(start_idx, len(self.matched_sets)):
                cam_dict = self.matched_sets[idx][1]
                if cam in cam_dict:
                    entries.append((idx, cam_dict[cam]))

            if len(entries) < 2:
                continue

            # CNNNN 순서로 비디오만 정렬
            videos_sorted = sorted(
                [video for _, video in entries],
                key=lambda v: self._get_cnnnn(v))

            # 원래 세트 순서대로 정렬된 비디오 재배치
            for i, (set_idx, _) in enumerate(entries):
                self.matched_sets[set_idx][1][cam] = videos_sorted[i]

    def get_rename_plan(self):
        plan = []
        for set_name, cam_dict in self.matched_sets:
            for cam, video in cam_dict.items():
                old_path = video.filepath
                new_filename = set_name + ".mp4"
                new_path = os.path.join(os.path.dirname(old_path), new_filename)
                organized_filename = cam + ".mp4"
                plan.append({
                    'set_name': set_name,
                    'cam': cam,
                    'old_path': old_path,
                    'old_filename': video.filename,
                    'new_filename': new_filename,
                    'new_path': new_path,
                    'organized_filename': organized_filename,
                    'organized_path': f"OrganizedVideos/{set_name}/{cam}/{organized_filename}",
                    'duration': video.duration,
                    'already_correct': (video.filename == new_filename)
                })
        return plan

    def execute_rename(self, plan, root_folder, progress_callback=None):
        success = 0
        errors = []
        rename_log = []  # 되돌리기용 로그
        total = len(plan)

        # 충돌 방지: 임시 이름으로 먼저 변경
        temp_plans = []
        for idx, item in enumerate(plan):
            if item['already_correct']:
                temp_plans.append(None)
                success += 1
                rename_log.append({
                    'cam': item['cam'], 'old': item['old_filename'],
                    'new': item['new_filename'], 'dir': os.path.dirname(item['old_path'])
                })
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
                rename_log.append({
                    'cam': item['cam'], 'old': item['old_filename'],
                    'new': item['new_filename'], 'dir': os.path.dirname(item['old_path'])
                })
            except Exception as e:
                errors.append(f"{item['new_filename']}: {e}")
                try:
                    os.rename(temp_plans[idx], item['old_path'])
                except:
                    pass

        return success, errors, rename_log

    @staticmethod
    def undo_rename(root_folder, progress_callback=None):
        """rename_log.json을 읽어 리네임을 되돌린다."""
        log_path = os.path.join(root_folder, "rename_log.json")
        if not os.path.exists(log_path):
            return 0, ["rename_log.json 파일을 찾을 수 없습니다."]

        with open(log_path, 'r', encoding='utf-8') as f:
            rename_log = json.load(f)

        success = 0
        errors = []
        total = len(rename_log)

        # 충돌 방지: 현재(new) → 임시 → 원본(old)
        temp_plans = []
        for idx, entry in enumerate(rename_log):
            if entry['old'] == entry['new']:
                temp_plans.append(None)
                success += 1
                continue
            temp_name = f"__temp_undo_{idx}__.mp4"
            temp_path = os.path.join(entry['dir'], temp_name)
            temp_plans.append(temp_path)

        # 1차: 현재 → 임시
        for idx, entry in enumerate(rename_log):
            if progress_callback:
                progress_callback(idx + 1, total * 2, f"임시 변경: {entry['new']}")
            if temp_plans[idx] is None:
                continue
            current_path = os.path.join(entry['dir'], entry['new'])
            try:
                os.rename(current_path, temp_plans[idx])
            except Exception as e:
                errors.append(f"{entry['new']}: {e}")
                temp_plans[idx] = None

        # 2차: 임시 → 원본
        for idx, entry in enumerate(rename_log):
            if progress_callback:
                progress_callback(total + idx + 1, total * 2, f"복원: {entry['old']}")
            if temp_plans[idx] is None:
                continue
            original_path = os.path.join(entry['dir'], entry['old'])
            try:
                os.rename(temp_plans[idx], original_path)
                success += 1
            except Exception as e:
                errors.append(f"{entry['old']}: {e}")
                try:
                    os.rename(temp_plans[idx], os.path.join(entry['dir'], entry['new']))
                except:
                    pass

        # 복원 완료 시 로그 삭제
        if not errors:
            try:
                os.remove(log_path)
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

    def _trim_video(self, input_path, output_path, target_frames):
        """영상을 target_frames까지만 저장 (시작 유지, 끝 트림). ffmpeg 우선, 없으면 OpenCV."""
        try:
            result = subprocess.run(
                ['ffmpeg', '-i', input_path, '-frames:v', str(target_frames),
                 '-c', 'copy', '-y', output_path],
                capture_output=True, timeout=600
            )
            if result.returncode == 0:
                return
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass

        # ffmpeg 없으면 OpenCV 재인코딩
        cap = cv2.VideoCapture(input_path)
        fps = cap.get(cv2.CAP_PROP_FPS)
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        out = cv2.VideoWriter(output_path, fourcc, fps, (width, height))
        for _ in range(target_frames):
            ret, frame = cap.read()
            if not ret:
                break
            out.write(frame)
        out.release()
        cap.release()

    def organize_files(self, output_folder="OrganizedVideos", copy_mode=True,
                       trim_mode=False, progress_callback=None):
        organized_count = 0
        trimmed_count = 0
        error_count = 0
        errors = []
        total_files = sum(len(v) for v in self.video_groups.values())
        current = 0
        output_path = os.path.join(self.root_folder, output_folder)

        for group_name, videos in self.video_groups.items():
            # 트림 모드: 그룹 내 최소 프레임 수 계산
            min_frames = None
            if trim_mode and len(videos) > 1:
                min_frames = min(v.frame_count for v in videos)

            for video in videos:
                current += 1
                needs_trim = (trim_mode and min_frames is not None
                              and video.frame_count > min_frames)
                try:
                    new_folder = os.path.join(output_path, group_name, video.folder)
                    os.makedirs(new_folder, exist_ok=True)
                    new_filename = video.folder + ".mp4"
                    new_filepath = os.path.join(new_folder, new_filename)

                    if needs_trim:
                        if progress_callback:
                            progress_callback(current, total_files,
                                f"트림 중: {video.folder}/{video.filename} "
                                f"({video.frame_count}→{min_frames}f)")
                        self._trim_video(video.filepath, new_filepath, min_frames)
                        trimmed_count += 1
                    else:
                        if progress_callback:
                            progress_callback(current, total_files,
                                f"처리 중: {video.folder}/{video.filename}")
                        if copy_mode:
                            shutil.copy2(video.filepath, new_filepath)
                        else:
                            shutil.move(video.filepath, new_filepath)

                    organized_count += 1
                except Exception as e:
                    error_count += 1
                    errors.append(f"{video.folder}/{video.filename}: {str(e)}")
        return organized_count, trimmed_count, error_count, errors


# ─────────────────────────────────────────────
# 5. SetGridViewer: 세트 그리드 프리뷰어
# ─────────────────────────────────────────────
class SetGridViewer(tk.Toplevel):
    DECODE_WIDTH = 480  # 디코딩 후 즉시 리사이즈할 최대 폭

    def __init__(self, parent, matched_sets, root_folder=None, cam_folders=None):
        super().__init__(parent)
        self.title("세트 프리뷰어 - 프레임 비교")
        # 화면 크기에 맞춰 창 크기 설정 (작업표시줄 고려)
        screen_w = self.winfo_screenwidth()
        screen_h = self.winfo_screenheight()
        win_w = min(1200, screen_w - 40)
        win_h = min(800, screen_h - 80)
        self.geometry(f"{win_w}x{win_h}")
        self.matched_sets = matched_sets
        self.cam_folders = cam_folders or {}  # {cam: [VideoInfo, ...]}
        self.current_set_idx = 0
        self.current_frame = 0
        self.captures = {}
        self.photo_images = []
        self.playing = False
        self.play_speed = 33      # ms (~30fps, 1x)
        self.frame_step = 1
        self.step_buttons = {}    # {step_value: Button}
        self.speed_buttons = {}   # {ms_value: Button}

        # 카메라별 파일 선택 UI
        self.file_combos = {}     # {cam: Combobox}
        self.file_maps = {}       # {cam: {display_str: VideoInfo}}
        self._set_modified = False

        # 병렬 디코딩 & 프리페치
        self.decode_executor = None
        self.display_sizes = {}       # {cam: (w, h)} 캐시
        self._prefetch_data = None    # 미리 디코딩된 프레임 데이터
        self._prefetch_frame_no = -1
        self._prefetch_thread = None
        self._prefetching = False
        self._cam_next_frame = {}     # {cam: 다음 예상 프레임} seek 생략용

        self._setup_ui()

        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.bind('<Left>', lambda e: self._prev_frame())
        self.bind('<Right>', lambda e: self._next_frame())
        self.bind('<space>', lambda e: self._toggle_play())
        self.bind('<Up>', lambda e: self._prev_set())
        self.bind('<Down>', lambda e: self._next_set())

        # 창을 먼저 렌더링한 후 첫 세트 로드
        self.update()
        self.after(50, lambda: self._load_set(0))

    def _setup_ui(self):
        # 상단 컨트롤
        top = tk.Frame(self, bg='#333')
        top.pack(fill=tk.X)

        tk.Button(top, text="◀◀", command=self._prev_set,
                  bg='#555', fg='white', width=5).pack(side=tk.LEFT, padx=3, pady=5)

        # 세트 번호 직접 이동
        self.set_spin_var = tk.IntVar(value=1)
        self.set_spin = tk.Spinbox(
            top, from_=1, to=max(len(self.matched_sets), 1), width=5,
            textvariable=self.set_spin_var, bg='#444', fg='white',
            font=('Arial', 12, 'bold'))
        self.set_spin.pack(side=tk.LEFT, padx=2, pady=5)
        self.set_spin.bind('<Return>', lambda e: self._goto_set())
        tk.Button(top, text="이동", command=self._goto_set,
                  bg='#555', fg='white', width=4).pack(side=tk.LEFT, padx=2)

        self.set_label = tk.Label(top, text="", bg='#333', fg='white',
                                  font=('Arial', 12, 'bold'))
        self.set_label.pack(side=tk.LEFT, expand=True)

        # 수정 반영 버튼
        self.apply_btn = tk.Button(top, text="수정 반영", command=self._apply_changes,
                                   bg='#FF5722', fg='white', width=8,
                                   state=tk.DISABLED)
        self.apply_btn.pack(side=tk.RIGHT, padx=5, pady=5)
        tk.Button(top, text="▶▶", command=self._next_set,
                  bg='#555', fg='white', width=5).pack(side=tk.RIGHT, padx=3, pady=5)

        # 영상 그리드 + 하단 컨트롤 (PanedWindow: 드래그로 비율 조절 가능)
        self.viewer_paned = tk.PanedWindow(self, orient=tk.VERTICAL,
                                           sashwidth=6, sashrelief=tk.RAISED,
                                           bg='#666')
        self.viewer_paned.pack(fill=tk.BOTH, expand=True)

        self.grid_frame = tk.Frame(self.viewer_paned, bg='black')
        self.viewer_paned.add(self.grid_frame, stretch='always')

        # 하단 컨트롤
        bottom = tk.Frame(self.viewer_paned, bg='#333')
        self.viewer_paned.add(bottom, stretch='never', minsize=180, height=200)

        # 재생 컨트롤 버튼
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

        # 프레임 스텝 선택
        step_frame = tk.Frame(bottom, bg='#333')
        step_frame.pack(pady=3)
        tk.Label(step_frame, text="프레임 스텝:", bg='#333', fg='white').pack(side=tk.LEFT)
        for step in [1, 5, 10, 15, 30, 50]:
            btn = tk.Button(step_frame, text=str(step), width=4,
                            bg='#555', fg='white',
                            command=lambda s=step: self._set_step(s))
            btn.pack(side=tk.LEFT, padx=2)
            self.step_buttons[step] = btn
        # 기본 선택 하이라이트
        self.step_buttons[1].config(bg='#4CAF50')

        # 재생 속도 선택
        speed_frame = tk.Frame(bottom, bg='#333')
        speed_frame.pack(pady=3)
        tk.Label(speed_frame, text="속도:", bg='#333', fg='white').pack(side=tk.LEFT)
        for label, ms in [("0.25x", 132), ("0.5x", 66), ("1x", 33), ("2x", 17)]:
            btn = tk.Button(speed_frame, text=label, width=5, bg='#555', fg='white',
                            command=lambda m=ms: self._set_speed(m))
            btn.pack(side=tk.LEFT, padx=2)
            self.speed_buttons[ms] = btn
        # 기본 선택 하이라이트
        self.speed_buttons[33].config(bg='#2196F3')

    def _release_captures(self):
        # 진행 중인 프리페치 완료 대기
        if self._prefetching and self._prefetch_thread is not None:
            self._prefetch_thread.join(timeout=3)
        self._prefetching = False
        self._prefetch_data = None
        self._prefetch_frame_no = -1
        self._cam_next_frame.clear()

        for cap in self.captures.values():
            if cap is not None:
                cap.release()
        self.captures.clear()

        if self.decode_executor is not None:
            self.decode_executor.shutdown(wait=False)
            self.decode_executor = None

    def _load_set(self, idx):
        self.playing = False
        self.play_btn.config(text="▶")
        self._release_captures()

        if idx < 0 or idx >= len(self.matched_sets):
            return

        self.current_set_idx = idx
        self.set_spin_var.set(idx + 1)
        self.current_frame = 0
        set_name, cam_dict = self.matched_sets[idx]

        self.set_label.config(
            text=f"{set_name}  ({idx + 1}/{len(self.matched_sets)})  로딩 중...")
        self.update()

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

        # 병렬 디코딩용 스레드 풀 생성
        n_workers = min(len(self.cam_names), os.cpu_count() or 4)
        self.decode_executor = ThreadPoolExecutor(max_workers=n_workers)
        self._cam_next_frame.clear()
        self.display_sizes.clear()

        # 그리드 레이아웃 계산
        for w in self.grid_frame.winfo_children():
            w.destroy()

        n = len(self.cam_names)
        cols = min(n, 3)
        rows = math.ceil(n / cols)

        self.grid_labels = {}
        self.grid_name_labels = {}
        self.file_combos = {}
        self.file_maps = {}
        self._set_modified = False
        self.apply_btn.config(state=tk.DISABLED)

        for i, cam in enumerate(self.cam_names):
            r, c = divmod(i, cols)
            cell = tk.Frame(self.grid_frame, bg='black', bd=1, relief=tk.SOLID)
            cell.grid(row=r * 2, column=c, rowspan=2, sticky='nsew', padx=2, pady=2)

            video = cam_dict[cam]
            name_lbl = tk.Label(cell, text=f"{cam}: {video.filename} ({video.duration:.2f}s)",
                                bg='#222', fg='white', font=('Arial', 9))
            name_lbl.pack(fill=tk.X)
            self.grid_name_labels[cam] = name_lbl

            img_lbl = tk.Label(cell, bg='black', text='로딩 중...', fg='gray',
                               cursor='hand2')
            img_lbl.pack(fill=tk.BOTH, expand=True)
            img_lbl.bind('<Double-1>',
                         lambda e, c=cam: self._open_trimmer(c))
            self.grid_labels[cam] = img_lbl

            # 파일 선택 Combobox (cam_folders가 있을 때만)
            if cam in self.cam_folders:
                self.file_maps[cam] = {}
                display_list = []
                current_display = ""
                for v in self.cam_folders[cam]:
                    display = f"{v.filename}  ({v.duration:.2f}s)"
                    display_list.append(display)
                    self.file_maps[cam][display] = v
                    if v.filepath == video.filepath:
                        current_display = display

                combo_row = tk.Frame(cell, bg='black')
                combo_row.pack(fill=tk.X, padx=2, pady=(0, 2))

                combo = ttk.Combobox(combo_row, values=display_list,
                                     state='readonly', width=35,
                                     font=('Arial', 8))
                if current_display:
                    combo.set(current_display)
                combo.pack(side=tk.LEFT, fill=tk.X, expand=True)
                combo.bind('<<ComboboxSelected>>',
                           lambda e, c=cam: self._on_file_change(c))
                self.file_combos[cam] = combo

                tk.Button(combo_row, text="...", width=3,
                          bg='#555', fg='white', font=('Arial', 8),
                          command=lambda c=cam: self._browse_file(c)
                          ).pack(side=tk.LEFT, padx=1)

        for c in range(cols):
            self.grid_frame.columnconfigure(c, weight=1)
        for r in range(rows * 2):
            self.grid_frame.rowconfigure(r, weight=1)

        # 그리드를 먼저 보여준 뒤 첫 프레임 비동기 디코딩
        self.update()
        self._load_first_frame_async()

    def _load_first_frame_async(self):
        """첫 프레임을 백그라운드에서 디코딩하여 UI를 블로킹하지 않는다.
        프리페치 플래그를 사용하여 재생 시작 전 완료를 보장한다."""
        self._prefetching = True

        def do_decode():
            try:
                data = self._decode_all_parallel(0)
                self.after(0, lambda d=data: self._apply_frame_data(d, 0))
            finally:
                self._prefetching = False

        self._prefetch_thread = threading.Thread(target=do_decode, daemon=True)
        self._prefetch_thread.start()

    def _apply_frame_data(self, frame_data, frame_no):
        """디코딩된 프레임 데이터를 UI에 반영한다."""
        self.current_frame = frame_no
        self.photo_images.clear()
        for cam in self.cam_names:
            label = self.grid_labels.get(cam)
            if label is None:
                continue
            data = frame_data.get(cam)
            if data is not None:
                img = Image.fromarray(data)
                photo = ImageTk.PhotoImage(img)
                self.photo_images.append(photo)
                label.config(image=photo, text='')
            else:
                label.config(image='', text='END', fg='gray')

        self.frame_label.config(text=f"Frame: {frame_no}/{self.max_frames}")
        self.frame_slider.set(frame_no)
        set_name = self.matched_sets[self.current_set_idx][0]
        self.set_label.config(
            text=f"{set_name}  ({self.current_set_idx + 1}/{len(self.matched_sets)})")

    # ── 병렬 디코딩 엔진 ──
    #
    # 최적화 전략:
    #   1) grab/retrieve 분리 — grab()은 디코딩만, retrieve()는 마지막 1장만
    #   2) 프레임 스텝 시 중간 프레임은 grab()으로 빠르게 건너뜀
    #   3) 디코딩 직후 DECODE_WIDTH 크기로 축소 (메모리·처리량 감소)
    #   4) 모든 카메라를 ThreadPoolExecutor로 병렬 처리
    #   5) 연속 재생 시 seek 생략 + 다음 프레임 프리페치

    def _decode_cam_frame(self, cam, frame_no):
        """단일 카메라 프레임 디코딩 (워커 스레드에서 실행)"""
        cap = self.captures.get(cam)
        if cap is None:
            return cam, None

        expected = self._cam_next_frame.get(cam, -1)

        if expected != frame_no:
            # 불연속 — seek 필요
            cap.set(cv2.CAP_PROP_POS_FRAMES, frame_no)
        elif expected == frame_no and self.frame_step > 1:
            # 연속이지만 스텝 > 1 — 이전 _show_frame에서 이미 step-1만큼
            # grab 해뒀으므로 여기서는 바로 retrieve
            pass

        ret, frame = cap.read()
        if not ret:
            return cam, None

        self._cam_next_frame[cam] = frame_no + 1

        # 즉시 저해상도로 축소 (이후 처리량 대폭 감소)
        h, w = frame.shape[:2]
        if w > self.DECODE_WIDTH:
            scale = self.DECODE_WIDTH / w
            new_w = self.DECODE_WIDTH
            new_h = max(int(h * scale), 1)
            frame = cv2.resize(frame, (new_w, new_h),
                               interpolation=cv2.INTER_NEAREST)

        # 디스플레이 크기에 맞춰 최종 리사이즈 + 색 변환
        lw, lh = self.display_sizes.get(cam, (380, 280))
        h, w = frame.shape[:2]
        scale = min(lw / w, lh / h)
        new_w = max(int(w * scale), 1)
        new_h = max(int(h * scale), 1)

        frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        if (new_w, new_h) != (w, h):
            frame_rgb = cv2.resize(frame_rgb, (new_w, new_h),
                                   interpolation=cv2.INTER_NEAREST)
        return cam, frame_rgb

    def _grab_skip(self, steps):
        """모든 카메라에서 steps 프레임을 grab()으로 빠르게 건너뛴다.
        grab()은 프레임 데이터를 메모리에 복사하지 않아 read()보다 훨씬 빠름."""
        if steps <= 0:
            return

        def grab_cam(cam):
            cap = self.captures.get(cam)
            if cap is None:
                return
            for _ in range(steps):
                if not cap.grab():
                    break
            # grab 후 다음 예상 프레임 갱신
            self._cam_next_frame[cam] = (
                self._cam_next_frame.get(cam, 0) + steps)

        futures = [self.decode_executor.submit(grab_cam, cam)
                   for cam in self.cam_names]
        for f in futures:
            f.result()

    def _decode_all_parallel(self, frame_no):
        """모든 카메라를 병렬로 디코딩"""
        futures = {cam: self.decode_executor.submit(
                       self._decode_cam_frame, cam, frame_no)
                   for cam in self.cam_names}
        results = {}
        for cam, future in futures.items():
            _, data = future.result()
            results[cam] = data
        return results

    def _start_prefetch(self, frame_no):
        """다음 프레임을 백그라운드에서 미리 디코딩"""
        if frame_no < 0 or frame_no > self.max_frames:
            return
        if self._prefetching:
            return

        def do_prefetch():
            try:
                data = self._decode_all_parallel(frame_no)
                self._prefetch_data = data
                self._prefetch_frame_no = frame_no
            finally:
                self._prefetching = False

        self._prefetching = True
        self._prefetch_thread = threading.Thread(target=do_prefetch, daemon=True)
        self._prefetch_thread.start()

    def _show_frame(self, frame_no):
        if not self.captures:
            return
        self.current_frame = max(0, min(frame_no, self.max_frames))

        # 디스플레이 크기 캐시 갱신 (메인 스레드에서만 tkinter 접근)
        for cam in self.cam_names:
            label = self.grid_labels.get(cam)
            if label:
                lw = label.winfo_width()
                lh = label.winfo_height()
                if lw < 10:
                    lw = 380
                if lh < 10:
                    lh = 280
                self.display_sizes[cam] = (lw, lh)

        # 프리페치 완료 대기 (캡처 객체 충돌 방지)
        if self._prefetching and self._prefetch_thread is not None:
            self._prefetch_thread.join()
            self._prefetching = False

        # 프리페치 데이터 사용 또는 새로 디코딩
        frame_data = None
        if self._prefetch_frame_no == self.current_frame and self._prefetch_data is not None:
            frame_data = self._prefetch_data
            self._prefetch_data = None
            self._prefetch_frame_no = -1

        if frame_data is None:
            frame_data = self._decode_all_parallel(self.current_frame)

        self._apply_frame_data(frame_data, self.current_frame)

    def _on_slider(self, val):
        frame_no = int(float(val))
        if frame_no != self.current_frame:
            self._wait_for_pending_decode()
            self._show_frame(frame_no)

    def _wait_for_pending_decode(self):
        """진행 중인 백그라운드 디코딩(프리페치/첫프레임) 완료를 대기한다."""
        if self._prefetching and self._prefetch_thread is not None:
            self._prefetch_thread.join()
            self._prefetching = False

    def _advance_step(self):
        """현재 위치에서 frame_step만큼 전진.
        연속 흐름이면 grab()으로 중간 프레임을 빠르게 건너뛴다."""
        next_frame = self.current_frame + self.frame_step
        if next_frame > self.max_frames:
            return -1

        # 캡처 충돌 방지: 백그라운드 디코딩 완료 대기
        self._wait_for_pending_decode()

        # 연속인지 확인 (모든 카메라의 다음 예상 프레임이 일치)
        is_sequential = all(
            self._cam_next_frame.get(cam, -1) == self.current_frame + 1
            for cam in self.cam_names)

        if is_sequential and self.frame_step > 1:
            # 중간 (step-1)장을 grab()으로 빠르게 건너뜀
            self._grab_skip(self.frame_step - 1)

        self._show_frame(next_frame)
        return next_frame

    def _next_frame(self):
        self._advance_step()

    def _prev_frame(self):
        self._show_frame(self.current_frame - self.frame_step)

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
        result = self._advance_step()
        if result < 0:
            self.playing = False
            self.play_btn.config(text="▶")
            return
        # 재생 중 다음 프레임 미리 프리페치
        prefetch_target = self.current_frame + self.frame_step
        if prefetch_target <= self.max_frames:
            self._start_prefetch(prefetch_target)
        self.after(self.play_speed, self._play_loop)

    def _set_step(self, step):
        self.frame_step = step
        for s, btn in self.step_buttons.items():
            btn.config(bg='#4CAF50' if s == step else '#555')

    def _set_speed(self, ms):
        self.play_speed = ms
        for m, btn in self.speed_buttons.items():
            btn.config(bg='#2196F3' if m == ms else '#555')

    # ── 세트 이동 & 파일 변경 ──

    def _goto_set(self):
        """Spinbox 값으로 세트 이동."""
        try:
            idx = self.set_spin_var.get() - 1
        except (tk.TclError, ValueError):
            return
        if 0 <= idx < len(self.matched_sets):
            self._load_set(idx)

    def _open_trimmer(self, cam):
        """더블클릭 시 해당 카메라의 현재 영상을 편집기로 연다."""
        set_name, cam_dict = self.matched_sets[self.current_set_idx]
        video = cam_dict.get(cam)
        if video:
            VideoTrimmerDialog(self, initial_file=video.filepath)

    def _on_file_change(self, cam):
        """카메라의 파일을 변경하고 프리뷰를 갱신한다 (임시 변경)."""
        combo = self.file_combos.get(cam)
        if not combo:
            return
        selected = combo.get()
        new_video = self.file_maps.get(cam, {}).get(selected)
        if not new_video:
            return

        self._wait_for_pending_decode()

        # 캡처 교체
        old_cap = self.captures.get(cam)
        if old_cap:
            old_cap.release()
        cap = cv2.VideoCapture(new_video.filepath)
        self.captures[cam] = cap

        # 이름 라벨 갱신
        self.grid_name_labels[cam].config(
            text=f"{cam}: {new_video.filename} ({new_video.duration:.2f}s)")

        # 프레임 카운트 갱신
        fc = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        if fc - 1 > self.max_frames:
            self.max_frames = fc - 1
            self.frame_slider.config(to=self.max_frames)

        # 수정 플래그
        self._set_modified = True
        self.apply_btn.config(state=tk.NORMAL)

        # 현재 프레임 새로 디코딩
        self._cam_next_frame[cam] = -1
        self._show_frame(self.current_frame)

    def _browse_file(self, cam):
        """파일 탐색기로 임의 mp4 파일을 선택하여 해당 카메라에 할당한다."""
        filepath = filedialog.askopenfilename(
            title=f"{cam} - 영상 파일 선택",
            filetypes=[("MP4 파일", "*.mp4"), ("모든 파일", "*.*")])
        if not filepath:
            return

        new_video = VideoInfo(filepath)
        if new_video.error:
            messagebox.showerror("오류", f"파일을 열 수 없습니다:\n{new_video.error}")
            return

        self._wait_for_pending_decode()

        # 캡처 교체
        old_cap = self.captures.get(cam)
        if old_cap:
            old_cap.release()
        cap = cv2.VideoCapture(new_video.filepath)
        self.captures[cam] = cap

        self.grid_name_labels[cam].config(
            text=f"{cam}: {new_video.filename} ({new_video.duration:.2f}s)")

        fc = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        if fc - 1 > self.max_frames:
            self.max_frames = fc - 1
            self.frame_slider.config(to=self.max_frames)

        # Combobox에 추가 (없는 파일일 수 있으므로)
        display = f"{new_video.filename}  ({new_video.duration:.2f}s)"
        if cam in self.file_maps:
            self.file_maps[cam][display] = new_video
            combo = self.file_combos.get(cam)
            if combo:
                vals = list(combo['values']) + [display]
                combo['values'] = vals
                combo.set(display)

        self._set_modified = True
        self.apply_btn.config(state=tk.NORMAL)

        self._cam_next_frame[cam] = -1
        self._show_frame(self.current_frame)

    def _apply_changes(self):
        """현재 세트의 파일 변경을 matched_sets에 확정 반영한다."""
        if not self._set_modified:
            return

        idx = self.current_set_idx
        set_name, cam_dict = self.matched_sets[idx]

        # 콤보박스 선택에 따라 cam_dict 갱신
        for cam, combo in self.file_combos.items():
            selected = combo.get()
            new_video = self.file_maps.get(cam, {}).get(selected)
            if new_video:
                cam_dict[cam] = new_video

        self._set_modified = False
        self.apply_btn.config(state=tk.DISABLED)
        self.set_label.config(
            text=f"{set_name}  ({idx + 1}/{len(self.matched_sets)})  반영 완료!")

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
        self.geometry("1100x600")
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

        columns = ('set', 'cam', 'old_name', 'new_name', 'final_name', 'duration')
        self.tree = ttk.Treeview(tree_frame, columns=columns, show='headings', height=20)

        self.tree.heading('set', text='세트')
        self.tree.heading('cam', text='카메라')
        self.tree.heading('old_name', text='현재 파일명')
        self.tree.heading('new_name', text='리네임')
        self.tree.heading('final_name', text='최종 정리 경로')
        self.tree.heading('duration', text='길이(초)')

        self.tree.column('set', width=70, anchor='center')
        self.tree.column('cam', width=70, anchor='center')
        self.tree.column('old_name', width=200)
        self.tree.column('new_name', width=120, anchor='center')
        self.tree.column('final_name', width=300)
        self.tree.column('duration', width=80, anchor='center')

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
                item['organized_path'],
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
# 8. CalibrationDialog: 캘리브레이션 영상 지정 (프리뷰 포함)
# ─────────────────────────────────────────────
class CalibrationDialog(tk.Toplevel):
    """각 카메라 폴더에서 렌즈 캘리브레이션 영상 1개를 선택하고
    첫 프레임 프리뷰로 확인하는 다이얼로그."""

    def __init__(self, parent, cam_folders, current_calibration=None):
        super().__init__(parent)
        self.title("캘리브레이션 영상 지정 — C0001")
        screen_w = self.winfo_screenwidth()
        screen_h = self.winfo_screenheight()
        win_w = min(1200, screen_w - 40)
        win_h = min(750, screen_h - 80)
        self.geometry(f"{win_w}x{win_h}")

        self.result = None  # None=취소, {}=해제, {cam:VideoInfo}=지정
        self.cam_folders = cam_folders
        self.current_calibration = current_calibration or {}
        self.combos = {}
        self.video_maps = {}       # {cam: {display_str: VideoInfo}}
        self.preview_labels = {}   # {cam: Label}
        self.photo_images = []     # GC 방지

        self.transient(parent)
        self.grab_set()
        self._setup_ui()

        # 초기 프리뷰 로드
        self.after(50, self._load_all_previews)
        self.wait_window(self)

    def _setup_ui(self):
        # 상단 안내
        header = tk.Frame(self, bg='#333')
        header.pack(fill=tk.X)
        tk.Label(header,
                 text="각 카메라에서 렌즈 캘리브레이션 영상을 선택하세요",
                 font=('Arial', 11, 'bold'), bg='#333', fg='white'
                 ).pack(pady=6)
        tk.Label(header,
                 text="선택된 영상은 C0001로 지정됩니다.  "
                      "동기화 매칭은 C0002부터 시작.",
                 bg='#333', fg='#aaa').pack(pady=(0, 6))

        # 하단 버튼 (먼저 pack하여 항상 표시)
        btn_frame = tk.Frame(self)
        btn_frame.pack(side=tk.BOTTOM, pady=10)

        tk.Button(btn_frame, text="확인", command=self._confirm,
                  width=12, height=2, bg='#4CAF50', fg='white'
                  ).pack(side=tk.LEFT, padx=10)
        tk.Button(btn_frame, text="해제", command=self._clear,
                  width=12, height=2, bg='#FF9800', fg='white'
                  ).pack(side=tk.LEFT, padx=10)
        tk.Button(btn_frame, text="취소", command=self._cancel,
                  width=12, height=2, bg='#f44336', fg='white'
                  ).pack(side=tk.LEFT, padx=10)

        # 카메라 그리드
        cam_names = sorted(self.cam_folders.keys(), key=natural_sort_key)
        n = len(cam_names)
        cols = min(n, 3)
        rows = math.ceil(n / cols)

        grid_frame = tk.Frame(self, bg='black')
        grid_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        for c in range(cols):
            grid_frame.columnconfigure(c, weight=1)
        for r in range(rows):
            grid_frame.rowconfigure(r, weight=1)

        for i, cam_name in enumerate(cam_names):
            r, c = divmod(i, cols)

            cell = tk.LabelFrame(grid_frame, text=cam_name,
                                 font=('Arial', 10, 'bold'),
                                 bg='#222', fg='white')
            cell.grid(row=r, column=c, padx=4, pady=4, sticky='nsew')

            # 프리뷰 이미지
            preview_lbl = tk.Label(cell, bg='black', text='로딩 중...',
                                   fg='gray', anchor='center')
            preview_lbl.pack(fill=tk.BOTH, expand=True, padx=3, pady=3)
            self.preview_labels[cam_name] = preview_lbl

            # 콤보박스
            videos = self.cam_folders[cam_name]
            self.video_maps[cam_name] = {}
            display_list = []
            for v in videos:
                display = f"{v.filename}  ({v.duration:.2f}초)"
                display_list.append(display)
                self.video_maps[cam_name][display] = v

            combo = ttk.Combobox(cell, values=display_list,
                                 state='readonly', width=45)
            # 기존 선택 또는 첫 번째 항목
            if cam_name in self.current_calibration:
                cal = self.current_calibration[cam_name]
                for display, video in self.video_maps[cam_name].items():
                    if video.filepath == cal.filepath:
                        combo.set(display)
                        break
            elif display_list:
                combo.current(0)

            combo.pack(fill=tk.X, padx=3, pady=(0, 4))
            combo.bind('<<ComboboxSelected>>',
                       lambda e, cn=cam_name: self._update_preview(cn))
            self.combos[cam_name] = combo

    # ── 프리뷰 ──

    def _load_all_previews(self):
        for cam_name in self.combos:
            self._update_preview(cam_name)

    def _update_preview(self, cam_name):
        combo = self.combos[cam_name]
        selected = combo.get()
        label = self.preview_labels[cam_name]

        if not selected or selected not in self.video_maps[cam_name]:
            label.config(image='', text='선택 없음', fg='gray')
            return

        video = self.video_maps[cam_name][selected]

        try:
            cap = cv2.VideoCapture(video.filepath)
            ret, frame = cap.read()
            cap.release()

            if not ret:
                label.config(image='', text='프레임 읽기 실패', fg='red')
                return

            # 라벨 크기에 맞춰 스케일링
            label.update_idletasks()
            lw = label.winfo_width()
            lh = label.winfo_height()
            if lw < 50:
                lw = 320
            if lh < 50:
                lh = 220

            h, w = frame.shape[:2]
            scale = min(lw / w, lh / h)
            new_w = max(int(w * scale), 1)
            new_h = max(int(h * scale), 1)

            frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            frame_resized = cv2.resize(frame_rgb, (new_w, new_h))

            img = Image.fromarray(frame_resized)
            photo = ImageTk.PhotoImage(img)
            self.photo_images.append(photo)
            label.config(image=photo, text='')
        except Exception as e:
            label.config(image='', text=f'오류: {e}', fg='red')

    # ── 결과 ──

    def _confirm(self):
        self.result = {}
        for cam_name, combo in self.combos.items():
            selected = combo.get()
            if selected and selected in self.video_maps[cam_name]:
                self.result[cam_name] = self.video_maps[cam_name][selected]
        self.destroy()

    def _clear(self):
        self.result = {}  # 빈 dict = 캘리브레이션 해제
        self.destroy()

    def _cancel(self):
        self.result = None  # None = 취소 (변경 없음)
        self.destroy()


# ─────────────────────────────────────────────
# 9. VideoTrimmerDialog: 영상 구간 편집기
# ─────────────────────────────────────────────
class VideoTrimmerDialog(tk.Toplevel):
    """MP4 영상에서 유지할 구간을 선택하고, 나머지를 잘라내어
    이어붙인 새 파일로 내보낸다. ffmpeg -c copy (무손실, 메타 동일)."""

    def __init__(self, parent, initial_file=None):
        super().__init__(parent)
        self.title("영상 구간 편집기")
        self.geometry("800x600")
        self.minsize(600, 450)

        self.filepath = None
        self.cap = None
        self.fps = 0
        self.frame_count = 0
        self.current_frame = 0
        self.playing = False
        self.photo_image = None
        self.segments = []
        self.mark_in = None

        self._setup_ui()
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.bind('<Left>', lambda e: self._step(-1))
        self.bind('<Right>', lambda e: self._step(1))
        self.bind('<space>', lambda e: self._toggle_play())
        self.bind('i', lambda e: self._set_mark_in())
        self.bind('o', lambda e: self._add_segment())

        if initial_file:
            self.after(50, lambda: self._load_file(initial_file))

    def _setup_ui(self):
        # ── 하단 고정 영역 (먼저 pack → 절대 안 잘림) ──
        bottom_fixed = tk.Frame(self)
        bottom_fixed.pack(side=tk.BOTTOM, fill=tk.X)

        # 내보내기 + 삭제
        export_row = tk.Frame(bottom_fixed, bg='#333')
        export_row.pack(fill=tk.X, padx=5, pady=3)
        tk.Button(export_row, text="내보내기 (무손실)",
                  command=self._export, bg='#9C27B0', fg='white',
                  width=18).pack(side=tk.RIGHT, padx=3)
        tk.Button(export_row, text="구간 삭제",
                  command=self._delete_segment, bg='#f44336', fg='white',
                  width=8).pack(side=tk.RIGHT, padx=3)
        tk.Label(export_row, text="ffmpeg -c copy: fps·해상도·코덱 100% 동일",
                 bg='#333', fg='#888', font=('Arial', 8)).pack(side=tk.LEFT, padx=5)

        # 구간 목록
        seg_frame = tk.LabelFrame(bottom_fixed, text="유지할 구간 (이 부분만 이어붙여 저장)")
        seg_frame.pack(fill=tk.X, padx=5, pady=2)
        self.seg_listbox = tk.Listbox(seg_frame, height=3, font=('Consolas', 9))
        self.seg_listbox.pack(fill=tk.X, padx=3, pady=3)

        # 구간 입력 (현재 프레임 버튼 + 직접 입력)
        mark = tk.Frame(bottom_fixed, bg='#444')
        mark.pack(fill=tk.X, padx=5, pady=2)

        tk.Button(mark, text="[ 현재→시작 (I)", command=self._set_mark_in,
                  bg='#FF9800', fg='white', width=14).pack(side=tk.LEFT, padx=2)
        tk.Button(mark, text="현재→끝 ] (O)", command=self._add_segment,
                  bg='#4CAF50', fg='white', width=12).pack(side=tk.LEFT, padx=2)

        tk.Label(mark, text="  직접입력:", bg='#444', fg='#aaa'
                 ).pack(side=tk.LEFT, padx=(10, 2))
        self.in_entry = tk.Entry(mark, width=12, font=('Consolas', 9))
        self.in_entry.pack(side=tk.LEFT, padx=1)
        self.in_entry.insert(0, "0")
        tk.Label(mark, text="~", bg='#444', fg='white').pack(side=tk.LEFT)
        self.out_entry = tk.Entry(mark, width=12, font=('Consolas', 9))
        self.out_entry.pack(side=tk.LEFT, padx=1)
        self.out_entry.insert(0, "0")

        # 단위 선택
        self.unit_var = tk.StringVar(value="frame")
        tk.Radiobutton(mark, text="프레임", variable=self.unit_var,
                       value="frame", bg='#444', fg='white',
                       selectcolor='#666').pack(side=tk.LEFT, padx=2)
        tk.Radiobutton(mark, text="초", variable=self.unit_var,
                       value="sec", bg='#444', fg='white',
                       selectcolor='#666').pack(side=tk.LEFT, padx=2)
        tk.Button(mark, text="추가", command=self._add_segment_manual,
                  bg='#2196F3', fg='white', width=5).pack(side=tk.LEFT, padx=3)

        # 재생 컨트롤 + 프레임 정보
        ctrl = tk.Frame(bottom_fixed, bg='#333')
        ctrl.pack(fill=tk.X, padx=5, pady=2)
        for txt, step in [("◀◀", -30), ("◀", -1)]:
            tk.Button(ctrl, text=txt, width=4, bg='#555', fg='white',
                      command=lambda s=step: self._step(s)).pack(side=tk.LEFT, padx=1)
        self.play_btn = tk.Button(ctrl, text="▶", width=6,
                                  command=self._toggle_play,
                                  bg='#4CAF50', fg='white')
        self.play_btn.pack(side=tk.LEFT, padx=1)
        for txt, step in [("▶", 1), ("▶▶", 30)]:
            tk.Button(ctrl, text=txt, width=4, bg='#555', fg='white',
                      command=lambda s=step: self._step(s)).pack(side=tk.LEFT, padx=1)
        self.frame_label = tk.Label(ctrl, text="Frame: 0/0  (00:00.000)",
                                    bg='#333', fg='white')
        self.frame_label.pack(side=tk.LEFT, padx=10)

        # 슬라이더
        self.frame_slider = tk.Scale(bottom_fixed, from_=0, to=0,
                                     orient=tk.HORIZONTAL, bg='#333', fg='white',
                                     highlightthickness=0, command=self._on_slider)
        self.frame_slider.pack(fill=tk.X, padx=5, pady=2)

        # ── 상단 영역 ──
        top = tk.Frame(self, bg='#333')
        top.pack(side=tk.TOP, fill=tk.X)
        tk.Button(top, text="파일 열기", command=self._open_file,
                  bg='#2196F3', fg='white', width=10).pack(side=tk.LEFT, padx=5, pady=4)
        self.file_label = tk.Label(top, text="파일을 선택하세요",
                                   bg='#333', fg='white', anchor='w')
        self.file_label.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)

        # ── 프리뷰 (나머지 공간 전부 차지) ──
        self.preview_label = tk.Label(self, bg='black', text='영상 없음',
                                      fg='gray', cursor='hand2')
        self.preview_label.pack(fill=tk.BOTH, expand=True, padx=5, pady=2)

    # ── 파일 ──

    def _open_file(self):
        filepath = filedialog.askopenfilename(
            filetypes=[("MP4 파일", "*.mp4"), ("모든 영상", "*.*")])
        if filepath:
            self._load_file(filepath)

    def _load_file(self, filepath):
        if self.cap:
            self.cap.release()
        self.filepath = filepath
        self.cap = cv2.VideoCapture(filepath)
        self.fps = self.cap.get(cv2.CAP_PROP_FPS)
        self.frame_count = int(self.cap.get(cv2.CAP_PROP_FRAME_COUNT))
        self.current_frame = 0
        self.segments.clear()
        self.mark_in = None
        self.seg_listbox.delete(0, tk.END)

        # ffprobe로 정확한 fps 비율 + timescale 감지
        self.r_frame_rate = None   # "180000/1001" 같은 정확한 비율
        self.timescale = None
        try:
            r = subprocess.run([
                'ffprobe', '-v', 'error', '-select_streams', 'v:0',
                '-show_entries', 'stream=r_frame_rate',
                '-of', 'csv=p=0', filepath
            ], capture_output=True, text=True, timeout=10)
            if r.returncode == 0:
                rate = r.stdout.strip().split('\n')[0]
                if '/' in rate:
                    num, den = map(int, rate.split('/'))
                    self.r_frame_rate = rate
                    self.timescale = num
                    self.fps = num / den  # OpenCV보다 정확한 fps
        except (FileNotFoundError, Exception):
            pass

        w = int(self.cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        h = int(self.cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        fps_display = self.r_frame_rate or f"{self.fps:.3f}"
        self.file_label.config(
            text=f"{os.path.basename(filepath)}  |  {w}x{h}  |  "
                 f"{fps_display} fps  |  {self.frame_count}f")
        self.frame_slider.config(to=max(self.frame_count - 1, 0))
        self._show_frame(0)

    # ── 재생 ──

    def _frame_to_time(self, f):
        if self.fps <= 0:
            return "00:00.000"
        s = f / self.fps
        return f"{int(s // 60):02d}:{s % 60:06.3f}"

    def _show_frame(self, frame_no):
        if not self.cap:
            return
        self.current_frame = max(0, min(frame_no, self.frame_count - 1))
        self.cap.set(cv2.CAP_PROP_POS_FRAMES, self.current_frame)
        ret, frame = self.cap.read()
        if not ret:
            return

        self.preview_label.update_idletasks()
        lw = max(self.preview_label.winfo_width(), 320)
        lh = max(self.preview_label.winfo_height(), 180)
        h, w = frame.shape[:2]
        scale = min(lw / w, lh / h)
        nw, nh = max(int(w * scale), 1), max(int(h * scale), 1)

        rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        rgb = cv2.resize(rgb, (nw, nh), interpolation=cv2.INTER_AREA)
        self.photo_image = ImageTk.PhotoImage(Image.fromarray(rgb))
        self.preview_label.config(image=self.photo_image, text='')

        self.frame_label.config(
            text=f"Frame: {self.current_frame}/{self.frame_count}  "
                 f"({self._frame_to_time(self.current_frame)})")
        self.frame_slider.set(self.current_frame)

    def _on_slider(self, val):
        f = int(float(val))
        if f != self.current_frame:
            self._show_frame(f)

    def _step(self, n):
        self._show_frame(self.current_frame + n)

    def _toggle_play(self):
        self.playing = not self.playing
        self.play_btn.config(text="⏸" if self.playing else "▶")
        if self.playing:
            self._play_loop()

    def _play_loop(self):
        if not self.playing or self.current_frame >= self.frame_count - 1:
            self.playing = False
            self.play_btn.config(text="▶")
            return
        self._show_frame(self.current_frame + 1)
        delay = max(int(1000 / self.fps), 1) if self.fps > 0 else 33
        self.after(delay, self._play_loop)

    # ── 구간 선택 ──

    def _set_mark_in(self):
        """현재 프레임을 시작점으로 설정하고 입력창에도 반영."""
        self.mark_in = self.current_frame
        self.in_entry.delete(0, tk.END)
        self.in_entry.insert(0, str(self.current_frame))

    def _add_segment(self):
        """현재 프레임을 끝점으로, mark_in을 시작점으로 구간 추가."""
        if self.mark_in is None:
            messagebox.showwarning("경고", "먼저 시작점 [I]을 설정하세요.")
            return
        end = self.current_frame
        if end <= self.mark_in:
            messagebox.showwarning("경고", "끝점은 시작점보다 뒤여야 합니다.")
            return
        self.segments.append((self.mark_in, end))
        self.segments.sort()
        self.mark_in = None
        self._refresh_segments()

    def _parse_time_input(self, text):
        """입력값을 프레임 번호로 변환.
        '12345' → 프레임 번호, '01:23.456' → 초→프레임 변환.
        unit_var에 따라 해석."""
        text = text.strip()
        if not text:
            return None
        try:
            if self.unit_var.get() == "sec":
                # 초 단위: '90.5' 또는 '01:30.5'
                if ':' in text:
                    parts = text.split(':')
                    seconds = float(parts[0]) * 60 + float(parts[1])
                else:
                    seconds = float(text)
                return max(0, int(seconds * self.fps)) if self.fps > 0 else 0
            else:
                return max(0, int(float(text)))
        except ValueError:
            return None

    def _add_segment_manual(self):
        """입력창의 시작/끝 값으로 구간 직접 추가."""
        start = self._parse_time_input(self.in_entry.get())
        end = self._parse_time_input(self.out_entry.get())
        if start is None or end is None:
            messagebox.showwarning("경고", "시작/끝 값을 올바르게 입력하세요.\n"
                                   "프레임: 12345  |  초: 90.5 또는 01:30.5")
            return
        if end <= start:
            messagebox.showwarning("경고", "끝 값이 시작보다 커야 합니다.")
            return
        self.segments.append((start, end))
        self.segments.sort()
        self._refresh_segments()

    def _refresh_segments(self):
        self.seg_listbox.delete(0, tk.END)
        for i, (s, e) in enumerate(self.segments):
            dur = (e - s) / self.fps if self.fps > 0 else 0
            ss = s / self.fps if self.fps > 0 else 0
            se = e / self.fps if self.fps > 0 else 0
            self.seg_listbox.insert(tk.END,
                f"  {i + 1}.  F{s}~F{e} ({e - s}f)  |  "
                f"{ss:.3f}s~{se:.3f}s ({dur:.3f}s)")

    def _delete_segment(self):
        sel = self.seg_listbox.curselection()
        if sel:
            del self.segments[sel[0]]
            self._refresh_segments()

    # ── 내보내기 ──

    def _get_ffprobe_rate(self, filepath):
        """ffprobe로 r_frame_rate 문자열 (예: '180000/1001')을 반환."""
        try:
            r = subprocess.run([
                'ffprobe', '-v', 'error', '-select_streams', 'v:0',
                '-show_entries', 'stream=r_frame_rate',
                '-of', 'csv=p=0', filepath
            ], capture_output=True, text=True, timeout=10)
            if r.returncode == 0:
                return r.stdout.strip().split('\n')[0]
        except Exception:
            pass
        return None

    def _export(self):
        if not self.filepath or not self.segments:
            messagebox.showwarning("경고", "파일과 유지 구간을 먼저 지정하세요.")
            return

        base = os.path.splitext(os.path.basename(self.filepath))[0]
        output = filedialog.asksaveasfilename(
            defaultextension=".mp4", filetypes=[("MP4", "*.mp4")],
            initialfile=f"{base}_trimmed.mp4")
        if not output:
            return

        # timescale 보존 인자
        ts_args = []
        if self.timescale:
            ts_args = ['-video_track_timescale', str(self.timescale)]

        import tempfile
        temp_dir = tempfile.mkdtemp()
        try:
            # 1단계: 구간 추출
            seg_files = []
            for i, (s, e) in enumerate(self.segments):
                seg_path = os.path.join(temp_dir, f"seg_{i:04d}.mp4")
                ss = s / self.fps if self.fps > 0 else 0
                duration = (e - s) / self.fps if self.fps > 0 else 0
                cmd = (['ffmpeg',
                        '-ss', f'{ss:.6f}',
                        '-i', self.filepath,
                        '-t', f'{duration:.6f}',
                        '-c', 'copy']
                       + ts_args + ['-y', seg_path])
                r = subprocess.run(cmd, capture_output=True)
                if r.returncode != 0 or not os.path.exists(seg_path):
                    # 타이밍 문제일 수 있으므로 미세 조정 후 재시도
                    duration = max(duration - 0.001, 0.001)
                    cmd = (['ffmpeg',
                            '-ss', f'{ss:.6f}',
                            '-i', self.filepath,
                            '-t', f'{duration:.6f}',
                            '-c', 'copy']
                           + ts_args + ['-y', seg_path])
                    r = subprocess.run(cmd, capture_output=True)
                    if r.returncode != 0 or not os.path.exists(seg_path):
                        messagebox.showerror("오류",
                            f"구간 {i + 1} 추출 실패:\n"
                            f"{r.stderr.decode(errors='replace')[:300]}")
                        return
                seg_files.append(seg_path)

            # 2단계: 병합
            list_path = os.path.join(temp_dir, "list.txt")
            with open(list_path, 'w', encoding='utf-8') as f:
                for p in seg_files:
                    f.write(f"file '{p.replace(os.sep, '/')}'\n")

            concat_path = os.path.join(temp_dir, "concat_out.mp4")
            cmd = (['ffmpeg', '-f', 'concat', '-safe', '0',
                    '-i', list_path, '-c', 'copy']
                   + ts_args + ['-y', concat_path])
            r = subprocess.run(cmd, capture_output=True)
            if r.returncode != 0:
                messagebox.showerror("오류",
                    f"병합 실패:\n{r.stderr.decode(errors='replace')[:300]}")
                return

            # 3단계: fps 검증 + 불일치 시 자동 보정
            orig_rate = self.r_frame_rate
            out_rate = self._get_ffprobe_rate(concat_path)

            if orig_rate and out_rate and orig_rate != out_rate:
                # fps 불일치 → 원본 fps를 강제 주입하는 보정 패스
                cmd = ['ffmpeg',
                       '-r', orig_rate,
                       '-i', concat_path,
                       '-c', 'copy']
                if self.timescale:
                    cmd += ['-video_track_timescale', str(self.timescale)]
                cmd += ['-y', output]
                subprocess.run(cmd, capture_output=True)

                # 보정 후 재검증
                fixed_rate = self._get_ffprobe_rate(output)
                if fixed_rate != orig_rate:
                    # 최종 수단: 재인코딩 없이 그냥 복사 (일부 경우 보정됨)
                    shutil.copy2(concat_path, output)
            else:
                shutil.copy2(concat_path, output)

            # 최종 결과
            final_rate = self._get_ffprobe_rate(output)
            total_f = sum(e - s for s, e in self.segments)
            rate_str = final_rate or "확인 불가"
            match = "일치" if (orig_rate and final_rate == orig_rate) else "확인 필요"

            messagebox.showinfo("내보내기 완료",
                f"{output}\n\n"
                f"{len(self.segments)}개 구간, {total_f}프레임\n"
                f"원본: {orig_rate or 'N/A'}\n"
                f"출력: {rate_str}\n"
                f"상태: {match}")
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def _on_close(self):
        self.playing = False
        if self.cap:
            self.cap.release()
        self.destroy()


# ─────────────────────────────────────────────
# 10. 메인 GUI
# ─────────────────────────────────────────────
class VideoOrganizerGUI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("다중 카메라 영상 정리 프로그램 v2")
        self.root.geometry("1100x850")

        self.organizer = VideoOrganizer()
        self.matcher = SetMatcher()
        self.history_manager = None
        self.video_files = []
        self.set_table_visible = False

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
        self.duration_var = tk.DoubleVar(value=999.0)
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

        self.trim_var = tk.BooleanVar(value=False)
        tk.Checkbutton(mode_frame, text="프레임 수 통일 (최소 기준 끝 트림)",
                       variable=self.trim_var).pack(side=tk.LEFT, padx=20)

        # 컨트롤 버튼 - 6단계
        control_frame = tk.Frame(self.root)
        control_frame.pack(fill=tk.X, padx=10, pady=10)

        tk.Button(control_frame, text="1. 스캔 & 매칭", command=self.step1_scan_match,
                  width=15, height=2, bg='#FF5722', fg='white').pack(side=tk.LEFT, padx=3)
        tk.Button(control_frame, text="캘리브 지정", command=self.set_calibration,
                  width=10, height=2, bg='#607D8B', fg='white').pack(side=tk.LEFT, padx=3)
        tk.Button(control_frame, text="2. 세트 프리뷰", command=self.step2_preview_sets,
                  width=15, height=2, bg='#E91E63', fg='white').pack(side=tk.LEFT, padx=3)
        tk.Button(control_frame, text="3. 리네임", command=self.step3_rename,
                  width=15, height=2, bg='#9C27B0', fg='white').pack(side=tk.LEFT, padx=3)
        tk.Button(control_frame, text="원본 복원", command=self.restore_original,
                  width=10, height=2, bg='#795548', fg='white').pack(side=tk.LEFT, padx=3)
        tk.Button(control_frame, text="4. 분석", command=self.step4_analyze,
                  width=15, height=2, bg='#FF9800', fg='white').pack(side=tk.LEFT, padx=3)
        tk.Button(control_frame, text="5. 검증", command=self.step5_validate,
                  width=15, height=2, bg='#2196F3', fg='white').pack(side=tk.LEFT, padx=3)
        tk.Button(control_frame, text="6. 정리 실행", command=self.step6_organize,
                  width=15, height=2, bg='#4CAF50', fg='white').pack(side=tk.LEFT, padx=3)
        tk.Button(control_frame, text="영상 편집", command=self.open_trimmer,
                  width=10, height=2, bg='#607D8B', fg='white').pack(side=tk.LEFT, padx=3)

        # 진행 상황
        self.progress_var = tk.StringVar(value="준비 중...")
        tk.Label(self.root, textvariable=self.progress_var, anchor='w').pack(fill=tk.X, padx=10)

        self.progress_bar = ttk.Progressbar(self.root, mode='determinate')
        self.progress_bar.pack(fill=tk.X, padx=10, pady=5)

        # 결과 영역 (PanedWindow: 텍스트 + 세트 테이블)
        self.content_paned = tk.PanedWindow(self.root, orient=tk.VERTICAL, sashwidth=5)
        self.content_paned.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))

        # 결과 텍스트
        result_frame = tk.LabelFrame(self.content_paned, text="결과")
        self.result_text = scrolledtext.ScrolledText(result_frame, wrap=tk.WORD)
        self.result_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        self.result_text.tag_config('header', font=('Arial', 12, 'bold'))
        self.result_text.tag_config('ok', foreground='green')
        self.result_text.tag_config('warning', foreground='orange')
        self.result_text.tag_config('error', foreground='red')
        self.result_text.tag_config('info', foreground='blue')
        self.result_text.tag_config('change', foreground='#9C27B0')

        self.content_paned.add(result_frame, stretch='always')

        # 세트 목록 테이블 (Step 1 이후 표시)
        self.set_table_frame = tk.LabelFrame(self.content_paned, text="세트 목록")
        self._setup_set_table()

    def _setup_set_table(self):
        """세트 목록 Treeview 및 프리뷰 버튼 구성"""
        # 버튼 행
        btn_row = tk.Frame(self.set_table_frame)
        btn_row.pack(fill=tk.X, padx=5, pady=5)

        tk.Button(btn_row, text="선택 항목 프리뷰", command=self._preview_selected_sets,
                  bg='#E91E63', fg='white', width=16).pack(side=tk.LEFT, padx=5)
        tk.Button(btn_row, text="전체 프리뷰", command=self._preview_all_sets,
                  bg='#E91E63', fg='white', width=12).pack(side=tk.LEFT, padx=5)

        # Treeview
        tree_frame = tk.Frame(self.set_table_frame)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=(0, 5))

        columns = ('set_name', 'cam_count', 'avg_duration', 'max_diff', 'status')
        self.set_tree = ttk.Treeview(tree_frame, columns=columns, show='headings',
                                     selectmode='extended', height=8)

        self.set_tree.heading('set_name', text='세트명')
        self.set_tree.heading('cam_count', text='카메라 수')
        self.set_tree.heading('avg_duration', text='평균 길이(초)')
        self.set_tree.heading('max_diff', text='최대 차이(초)')
        self.set_tree.heading('status', text='상태')

        self.set_tree.column('set_name', width=100, anchor='center')
        self.set_tree.column('cam_count', width=80, anchor='center')
        self.set_tree.column('avg_duration', width=150, anchor='center')
        self.set_tree.column('max_diff', width=150, anchor='center')
        self.set_tree.column('status', width=80, anchor='center')

        scrollbar = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.set_tree.yview)
        self.set_tree.configure(yscrollcommand=scrollbar.set)
        self.set_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.set_tree.tag_configure('normal', foreground='black')
        self.set_tree.tag_configure('warn', foreground='#FF8C00')

        # 더블클릭: 해당 세트 1개 프리뷰
        self.set_tree.bind('<Double-1>', self._on_set_double_click)

    def _populate_set_table(self):
        """매칭 결과로 세트 목록 테이블 채우기"""
        for item in self.set_tree.get_children():
            self.set_tree.delete(item)

        tolerance = self.duration_var.get()
        for idx, (set_name, cam_dict) in enumerate(self.matcher.matched_sets):
            cam_count = len(cam_dict)
            durations = [v.duration for v in cam_dict.values()]
            avg_dur = sum(durations) / len(durations)
            max_diff = max(durations) - min(durations) if len(durations) > 1 else 0
            status = "정상" if max_diff <= tolerance else "경고"
            tag = 'normal' if status == "정상" else 'warn'

            self.set_tree.insert('', tk.END, iid=str(idx), values=(
                set_name, cam_count, f"{avg_dur:.2f}", f"{max_diff:.3f}", status
            ), tags=(tag,))

    def _on_set_double_click(self, event):
        """세트 테이블 더블클릭: 해당 세트 1개만 프리뷰"""
        selection = self.set_tree.selection()
        if not selection:
            return
        idx = int(selection[0])
        if 0 <= idx < len(self.matcher.matched_sets):
            SetGridViewer(self.root, [self.matcher.matched_sets[idx]],
                         cam_folders=self.matcher.cam_folders)

    def _preview_selected_sets(self):
        """선택된 세트들만 프리뷰"""
        selection = self.set_tree.selection()
        if not selection:
            messagebox.showwarning("경고", "프리뷰할 세트를 선택하세요.\n"
                                   "(Ctrl+클릭 또는 Shift+클릭으로 다중 선택)")
            return
        indices = sorted(int(iid) for iid in selection)
        selected_sets = [self.matcher.matched_sets[i] for i in indices
                         if 0 <= i < len(self.matcher.matched_sets)]
        if selected_sets:
            SetGridViewer(self.root, selected_sets,
                         cam_folders=self.matcher.cam_folders)

    def _preview_all_sets(self):
        """전체 세트 프리뷰"""
        if not self.matcher.matched_sets:
            messagebox.showwarning("경고", "먼저 Step 1 (스캔 & 매칭)을 실행하세요.")
            return
        SetGridViewer(self.root, self.matcher.matched_sets,
                     cam_folders=self.matcher.cam_folders)

    # ── 폴더 선택 ──
    def select_folder(self):
        folder = filedialog.askdirectory()
        if folder:
            self.folder_label.config(text=folder)
            self.organizer.root_folder = folder

            # 히스토리 매니저 초기화 (디스크에서 기존 이력 자동 로드)
            self.history_manager = FileHistoryManager(folder)

            self.result_text.delete(1.0, tk.END)
            self.result_text.insert(tk.END, f"선택된 폴더: {folder}\n\n", 'header')

            # 기존 이력이 있으면 상태 표시
            if self.history_manager.has_history():
                summary = self.history_manager.get_change_summary()
                self.result_text.insert(tk.END,
                    f"[이력 감지] 원본 이력 존재: "
                    f"총 {summary['total_files']}개 파일 추적 중, "
                    f"{summary['changed_files']}개 변경됨, "
                    f"{summary['total_changes']}회 변경 기록\n\n", 'info')

            # 세트 테이블 초기화
            for item in self.set_tree.get_children():
                self.set_tree.delete(item)

            # 하위 폴더 탐색
            subfolders = sorted([
                d for d in os.listdir(folder)
                if os.path.isdir(os.path.join(folder, d))
                and d != FileHistoryManager.HISTORY_DIR
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

        # 원본 스냅샷 캡처 (최초 1회만 — 이후에는 기존 이력 유지)
        if self.history_manager:
            was_new = self.history_manager.capture_initial_state(cam_folders)
            if was_new:
                self.result_text.insert(tk.END,
                    "\n원본 상태 스냅샷 저장 완료 (.file_history/)\n", 'ok')
            else:
                summary = self.history_manager.get_change_summary()
                self.result_text.insert(tk.END,
                    f"\n기존 원본 이력 사용 중 "
                    f"({summary['changed_files']}개 파일 변경됨)\n", 'info')

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

        # 세트 목록 테이블 표시 및 채우기
        if not self.set_table_visible:
            self.content_paned.add(self.set_table_frame, stretch='always', height=250)
            self.set_table_visible = True
        self._populate_set_table()

    # ── 캘리브레이션 지정 ──
    def set_calibration(self):
        if not self.matcher.cam_folders:
            messagebox.showwarning("경고", "먼저 Step 1 (스캔 & 매칭)을 실행하세요.")
            return

        dialog = CalibrationDialog(self.root, self.matcher.cam_folders,
                                   self.matcher.calibration_videos)

        if dialog.result is None:  # 취소
            return

        self.matcher.set_calibration(dialog.result)

        # 재매칭
        self.result_text.delete(1.0, tk.END)

        if dialog.result:
            self.result_text.insert(tk.END,
                "캘리브레이션 영상 지정 완료 → C0001\n\n", 'header')
            for cam, video in sorted(dialog.result.items(),
                                     key=lambda x: natural_sort_key(x[0])):
                self.result_text.insert(tk.END,
                    f"  {cam}: {video.filename} ({video.duration:.2f}초)\n", 'info')
        else:
            self.result_text.insert(tk.END, "캘리브레이션 해제 완료\n\n", 'header')

        self.result_text.insert(tk.END, "\n세트 재매칭 중...\n", 'header')
        self.root.update_idletasks()

        matched = self.matcher.match_sets()

        self.result_text.insert(tk.END,
            f"\n매칭 결과: {len(matched)}개 세트\n\n", 'ok')

        for set_name, cam_dict in matched:
            durations = [v.duration for v in cam_dict.values()]
            max_diff = max(durations) - min(durations) if len(durations) > 1 else 0

            if set_name == "C0001" and self.matcher.calibration_videos:
                self.result_text.insert(tk.END,
                    f"  {set_name} [캘리브레이션]: ", 'header')
                self.result_text.insert(tk.END, "길이 무관\n", 'info')
            else:
                diff_tag = 'ok' if max_diff <= self.duration_var.get() else 'warning'
                self.result_text.insert(tk.END, f"  {set_name}: ", 'header')
                self.result_text.insert(tk.END,
                    f"최대 차이 {max_diff:.3f}초\n", diff_tag)

            for cam in sorted(cam_dict.keys(), key=natural_sort_key):
                v = cam_dict[cam]
                self.result_text.insert(tk.END,
                    f"    {cam}/{v.filename} -> {set_name}.mp4  "
                    f"({v.duration:.2f}초)\n", 'change')

        total_unmatched = sum(len(v) for v in self.matcher.unmatched.values())
        if total_unmatched > 0:
            self.result_text.insert(tk.END,
                f"\n미매칭 파일: {total_unmatched}개\n", 'warning')
            for cam, videos in self.matcher.unmatched.items():
                for v in videos:
                    self.result_text.insert(tk.END,
                        f"  {cam}/{v.filename} ({v.duration:.2f}초)\n", 'warning')

        self.progress_var.set(
            f"재매칭 완료: {len(matched)}개 세트, 미매칭 {total_unmatched}개")

        if not self.set_table_visible:
            self.content_paned.add(self.set_table_frame,
                                   stretch='always', height=250)
            self.set_table_visible = True
        self._populate_set_table()

    # ── Step 2: 세트 프리뷰 ──
    def step2_preview_sets(self):
        if not self.matcher.matched_sets:
            messagebox.showwarning("경고", "먼저 Step 1 (스캔 & 매칭)을 실행하세요.")
            return
        self._preview_all_sets()

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

            success, errors, rename_log = self.matcher.execute_rename(
                plan, self.organizer.root_folder, self.update_progress)

            # 히스토리에 변경 기록
            if self.history_manager and rename_log:
                self.history_manager.record_renames(rename_log)

            self.result_text.insert(tk.END, f"리네임 완료: 성공 {success}개\n", 'ok')
            if errors:
                self.result_text.insert(tk.END, f"오류 {len(errors)}개:\n", 'error')
                for err in errors:
                    self.result_text.insert(tk.END, f"  {err}\n", 'error')

            self.progress_var.set(f"리네임 완료: 성공 {success}개, 오류 {len(errors)}개")
        else:
            self.result_text.insert(tk.END, "\n리네임이 취소되었습니다.\n", 'warning')

    # ── 원본 복원 ──
    def restore_original(self):
        folder = self.organizer.root_folder
        if not folder or not os.path.isdir(folder):
            messagebox.showwarning("경고", "먼저 폴더를 선택하세요.")
            return

        if not self.history_manager or not self.history_manager.has_history():
            messagebox.showwarning("경고",
                "원본 이력이 없습니다.\n"
                "먼저 스캔(Step 1)을 실행하여 원본 상태를 기록하세요.")
            return

        summary = self.history_manager.get_change_summary()
        if summary['changed_files'] == 0:
            messagebox.showinfo("정보", "모든 파일이 이미 원본 상태입니다.")
            return

        # 복원 계획 미리보기
        plan = self.history_manager.get_restore_plan()
        preview_lines = []
        for item in plan[:20]:
            preview_lines.append(
                f"  {item['cam']}/{item['current']}  →  {item['original']}")
        if len(plan) > 20:
            preview_lines.append(f"  ... 외 {len(plan) - 20}개")

        resp = messagebox.askyesno("원본 복원",
            f"총 {summary['changed_files']}개 파일을 최초 원본 파일명으로 복원합니다.\n"
            f"(변경 이력: {summary['total_changes']}회)\n\n"
            + "\n".join(preview_lines) + "\n\n"
            "진행하시겠습니까?")
        if not resp:
            return

        self.result_text.delete(1.0, tk.END)
        self.result_text.insert(tk.END, "원본 복원 실행 중...\n\n", 'header')
        self.root.update_idletasks()

        success, errors = self.history_manager.restore_to_original(
            self.update_progress)

        self.result_text.insert(tk.END, f"원본 복원 완료: 성공 {success}개\n", 'ok')
        if errors:
            self.result_text.insert(tk.END, f"오류 {len(errors)}개:\n", 'error')
            for err in errors:
                self.result_text.insert(tk.END, f"  {err}\n", 'error')

        self.progress_var.set(
            f"원본 복원 완료: 성공 {success}개, 오류 {len(errors)}개")

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
            resp = messagebox.askyesnocancel("경고",
                f"동기화 오류가 있는 그룹 {len(error_groups)}개:\n"
                f"{', '.join(error_groups[:10])}\n\n"
                f"예 = 오류 포함 전체 진행\n"
                f"아니요 = 오류 그룹 제외하고 진행\n"
                f"취소 = 전체 취소")
            if resp is None:  # 취소
                return
            if resp is False:  # 아니요 = 오류 제외
                for name in error_groups:
                    if name in self.organizer.video_groups:
                        del self.organizer.video_groups[name]
            # True = 오류 포함 진행

        mode = "복사" if self.copy_mode_var.get() else "이동"
        trim = self.trim_var.get()
        trim_info = " + 프레임 트림" if trim else ""
        resp = messagebox.askyesno("최종 확인",
            f"파일을 {mode}하여 정리합니다.{trim_info}\n"
            f"출력 폴더: OrganizedVideos\n"
            f"대상: {len(self.organizer.video_groups)}개 그룹\n"
            f"진행하시겠습니까?")
        if not resp:
            return

        self.result_text.delete(1.0, tk.END)
        self.result_text.insert(tk.END, f"Step 6: 파일 정리 ({mode} 모드{trim_info})\n\n", 'header')

        organized, trimmed, error_count, error_details = self.organizer.organize_files(
            copy_mode=self.copy_mode_var.get(),
            trim_mode=trim,
            progress_callback=self.update_progress
        )

        self.result_text.insert(tk.END, f"\n정리 완료!\n", 'ok')
        self.result_text.insert(tk.END, f"성공: {organized}개 파일\n", 'info')
        if trimmed > 0:
            self.result_text.insert(tk.END, f"트림 적용: {trimmed}개 파일\n", 'info')
        if error_count > 0:
            self.result_text.insert(tk.END, f"실패: {error_count}개 파일\n", 'error')
            for err in error_details:
                self.result_text.insert(tk.END, f"  {err}\n", 'error')

        output_path = os.path.join(self.organizer.root_folder, 'OrganizedVideos')
        self.result_text.insert(tk.END, f"\n출력 폴더: {output_path}\n", 'info')

        messagebox.showinfo("완료",
            f"파일 정리 완료!\n성공: {organized}개 (트림: {trimmed}개), 실패: {error_count}개")

    def open_trimmer(self):
        VideoTrimmerDialog(self.root)

    def run(self):
        self.root.mainloop()


# ─────────────────────────────────────────────
# 진입점
# ─────────────────────────────────────────────
if __name__ == "__main__":
    import sys

    try:
        import cv2
        # 카메라별 병렬 디코딩을 직접 관리하므로 OpenCV 내부 스레드 제한
        cv2.setNumThreads(2)
    except ImportError:
        print("OpenCV가 설치되지 않았습니다.")
        print("설치: pip install opencv-python")
        sys.exit(1)

    try:
        from PIL import Image, ImageTk
    except ImportError:
        print("Pillow가 설치되지 않았습니다.")
        print("설치: pip install Pillow")
        sys.exit(1)

    app = VideoOrganizerGUI()
    app.run()
