from __future__ import annotations
import ast
import json
import os
import shutil
import subprocess
import ast, sys
import textwrap
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional
from json import JSONDecodeError
import re

import inspect
import random
from enum import Enum
import json
import csv
import logging
# Add parallel execution imports
import concurrent.futures
import threading
from collections import defaultdict

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

for h in list(logger.handlers):
    logger.removeHandler(h)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
run_id=None
# File handler
log_file = "agent.log"
file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

folders_moved=[]
try:
    import requests
except Exception as e:
    logger.error(f"error importing requests: moving it to a different folder..")
    shutil.move("requests","requests_new")
    folders_moved.append(["requests","requests_new"])
    import requests

# Add parallel execution classes
class PerformanceMonitor:
    """Monitor performance metrics for parallel operations"""
    def __init__(self):
        self.metrics = defaultdict(list)
        self.start_times = {}
    
    def start_timer(self, operation: str):
        """Start timing an operation"""
        self.start_times[operation] = time.time()
    
    def end_timer(self, operation: str):
        """End timing an operation and record the duration"""
        if operation in self.start_times:
            duration = time.time() - self.start_times[operation]
            self.metrics[operation].append(duration)
            logger.info(f"⏱️ {operation} took {duration:.2f} seconds")
    
    def get_average_time(self, operation: str) -> float:
        """Get average time for an operation"""
        times = self.metrics.get(operation, [])
        return sum(times) / len(times) if times else 0
    
    def get_performance_summary(self) -> str:
        """Get a summary of all performance metrics"""
        summary = "Performance Summary:\n"
        for operation, times in self.metrics.items():
            avg_time = sum(times) / len(times)
            total_time = sum(times)
            summary += f"  {operation}: avg={avg_time:.2f}s, total={total_time:.2f}s, count={len(times)}\n"
        return summary

class ParallelToolExecutor:
    """Execute multiple tool operations in parallel"""
    def __init__(self, tool_manager, max_workers=4):
        self.tool_manager = tool_manager
        self.max_workers = max_workers
        self.results = {}
        self.lock = threading.Lock()
    
    def execute_parallel_analysis(self, file_path: str, test_func_names: List[str]) -> Dict[str, Any]:
        """Execute multiple analysis tools in parallel"""
        
        tasks = {
            'test_coverage': lambda: self.tool_manager.analyze_test_coverage(test_func_names),
            'dependencies': lambda: self.tool_manager.analyze_dependencies(file_path),
            'code_smells': lambda: self.tool_manager.detect_code_smells(file_path),
            'git_history': lambda: self.tool_manager.analyze_git_history(file_path),
            'code_quality': lambda: self.tool_manager.get_code_quality_metrics(file_path)
        }
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_task = {
                executor.submit(task_func): task_name 
                for task_name, task_func in tasks.items()
            }
            
            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_task):
                task_name = future_to_task[future]
                try:
                    result = future.result(timeout=30)  # 30 second timeout per task
                    with self.lock:
                        self.results[task_name] = result
                    logger.info(f"✅ {task_name} completed successfully")
                except Exception as e:
                    with self.lock:
                        self.results[task_name] = f"Error: {str(e)}"
                    logger.error(f"❌ {task_name} failed: {e}")
        
        return self.results

class ParallelFileSearcher:
    """Search multiple files and terms in parallel"""
    def __init__(self, tool_manager):
        self.tool_manager = tool_manager
    
    def search_multiple_files_parallel(self, search_terms: List[str], file_patterns: List[str] = None) -> Dict[str, str]:
        """Search for multiple terms across files in parallel"""
        
        def search_single_term(term: str) -> tuple[str, str]:
            try:
                result = self.tool_manager.search_in_all_files_content_v2(
                    grep_search_command=f"grep -rn --include='*.py' . -e '{term}'"
                )
                return term, result
            except Exception as e:
                return term, f"Error searching for '{term}': {e}"
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(search_terms), 4)) as executor:
            future_to_term = {
                executor.submit(search_single_term, term): term 
                for term in search_terms
            }
            
            results = {}
            for future in concurrent.futures.as_completed(future_to_term):
                term, result = future.result()
                results[term] = result
        
        return results
    
    def search_multiple_directories_parallel(self, directories: List[str], search_term: str) -> Dict[str, str]:
        """Search the same term across multiple directories in parallel"""
        
        def search_directory(directory: str) -> tuple[str, str]:
            try:
                result = self.tool_manager.search_recurive_in_all_files_in_directory(
                    directory_path=directory,
                    search_term=search_term
                )
                return directory, result
            except Exception as e:
                return directory, f"Error searching in '{directory}': {e}"
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(directories), 3)) as executor:
            future_to_dir = {
                executor.submit(search_directory, directory): directory 
                for directory in directories
            }
            
            results = {}
            for future in concurrent.futures.as_completed(future_to_dir):
                directory, result = future.result()
                results[directory] = result
        
        return results

class ParallelFileProcessor:
    """Process multiple files in parallel"""
    def __init__(self, tool_manager):
        self.tool_manager = tool_manager
    
    def get_multiple_file_contents_parallel(self, file_paths: List[str]) -> Dict[str, str]:
        """Get contents of multiple files in parallel"""
        
        def get_file_content(file_path: str) -> tuple[str, str]:
            try:
                content = self.tool_manager.get_file_content(file_path)
                return file_path, content
            except Exception as e:
                return file_path, f"Error reading {file_path}: {e}"
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(file_paths), 5)) as executor:
            future_to_file = {
                executor.submit(get_file_content, file_path): file_path 
                for file_path in file_paths
            }
            
            results = {}
            for future in concurrent.futures.as_completed(future_to_file):
                file_path, content = future.result()
                results[file_path] = content
        
        return results
    
    def apply_multiple_edits_parallel(self, edits: List[Dict[str, Any]]) -> Dict[str, str]:
        """Apply multiple code edits in parallel"""
        
        def apply_single_edit(edit: Dict[str, Any]) -> tuple[str, str]:
            try:
                file_path = edit['file_path']
                search = edit['search']
                replace = edit['replace']
                
                result = self.tool_manager.apply_code_edit(
                    file_path=file_path,
                    search=search,
                    replace=replace
                )
                return file_path, result
            except Exception as e:
                return edit.get('file_path', 'unknown'), f"Error applying edit: {e}"
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(edits), 3)) as executor:
            future_to_edit = {
                executor.submit(apply_single_edit, edit): edit 
                for edit in edits
            }
            
            results = {}
            for future in concurrent.futures.as_completed(future_to_edit):
                file_path, result = future.result()
                results[file_path] = result
        
        return results

class DependencyAwareParallelExecutor:
    """Execute operations in parallel where possible, respecting dependencies"""
    def __init__(self, tool_manager):
        self.tool_manager = tool_manager
    
    def execute_with_dependencies(self, problem_statement: str, test_func_names: List[str]) -> Dict[str, Any]:
        """Execute operations in parallel where possible, respecting dependencies"""
        
        # Phase 1: Independent operations (can run in parallel)
        phase1_tasks = {
            'file_listing': lambda: self.tool_manager.list_python_files(),
            'git_status': lambda: self.tool_manager.get_git_status(),
            'git_branches': lambda: self.tool_manager.get_git_branches()
        }
        
        phase1_results = self._execute_parallel(phase1_tasks)
        
        # Phase 2: Operations that depend on Phase 1 results
        python_files = phase1_results.get('file_listing', '').split('\n')
        relevant_files = [f for f in python_files if f.strip()]
        
        phase2_tasks = {}
        for file_path in relevant_files[:5]:  # Limit to first 5 files
            phase2_tasks[f'analyze_{file_path}'] = lambda fp=file_path: self._analyze_file(fp)
        
        phase2_results = self._execute_parallel(phase2_tasks)
        
        # Phase 3: Operations that depend on test functions
        phase3_tasks = {}
        for test_func in test_func_names:
            file_path, func_name = test_func.split(" - ")
            phase3_tasks[f'test_analysis_{func_name}'] = lambda fp=file_path, fn=func_name: self._analyze_test(fp, fn)
        
        phase3_results = self._execute_parallel(phase3_tasks)
        
        return {
            'phase1': phase1_results,
            'phase2': phase2_results,
            'phase3': phase3_results
        }
    
    def _execute_parallel(self, tasks: Dict[str, callable]) -> Dict[str, Any]:
        """Execute a dictionary of tasks in parallel"""
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            future_to_task = {
                executor.submit(task_func): task_name 
                for task_name, task_func in tasks.items()
            }
            
            results = {}
            for future in concurrent.futures.as_completed(future_to_task):
                task_name = future_to_task[future]
                try:
                    result = future.result(timeout=60)
                    results[task_name] = result
                except Exception as e:
                    results[task_name] = f"Error: {e}"
        
        return results
    
    def _analyze_file(self, file_path: str) -> Dict[str, Any]:
        """Analyze a single file with multiple tools"""
        try:
            return {
                'content': self.tool_manager.get_file_content(file_path, limit=1000),
                'smells': self.tool_manager.detect_code_smells(file_path),
                'quality': self.tool_manager.get_code_quality_metrics(file_path)
            }
        except Exception as e:
            return {'error': str(e)}
    
    def _analyze_test(self, file_path: str, func_name: str) -> Dict[str, Any]:
        """Analyze a test function"""
        try:
            return {
                'body': self.tool_manager.get_function_body(file_path, func_name),
                'coverage': self.tool_manager.analyze_test_coverage([f"{file_path} - {func_name}"])
            }
        except Exception as e:
            return {'error': str(e)}

class COT:
    
    class Action:
            
        def __init__(self, next_thought: str, next_tool_name: str, next_tool_args: dict, observation: list|tuple|str,is_error:bool=False,raw_response:str=None,total_attempts:int=0,inference_error_counter:dict=None,request_data:list=None):
            self.next_thought=next_thought
            self.next_tool_name=next_tool_name
            self.next_tool_args=next_tool_args
            self.observation=";".join(observation) if isinstance(observation,list) else observation
            self.is_error=is_error
            self.raw_response=raw_response
            self.total_attempts=total_attempts
            self.inference_error_counter=inference_error_counter
            self.request_data=request_data
            self.is_deleted=False
    def __init__(self,latest_observations_to_keep=5):
        self.thoughts: list[COT.Action] = []
        self.latest_observations_to_keep=latest_observations_to_keep
        
    def add_action(self, action:COT.Action):
        for thought in self.thoughts:
            if thought.next_tool_name==action.next_tool_name and thought.next_tool_args==action.next_tool_args:
                thought.is_deleted=True
        self.thoughts.append(action)
        
    def to_str(self):
        messages=[]
        for i,thought in enumerate(self.thoughts):
            if thought.is_deleted:
                continue
            if i<len(self.thoughts)-self.latest_observations_to_keep:
                assistant_str = (
                    f"next_thought:{thought.next_thought}\n"
                    f"next_tool_name:{thought.next_tool_name}\n"
                    f"next_tool_args:{thought.next_tool_args}\n"
                )
                user_str=( f"observation: {'error ocurred.' if thought.is_error else ''} "
                    f"output omitted ({len(thought.observation.splitlines()) if thought.observation is not None else 0}) lines\n")
                
            else:
                if thought.is_error is None or i==len(self.thoughts)-1:
                    assistant_str=f"next_thought:{thought.next_thought}\nnext_tool_name:{thought.next_tool_name}\nnext_tool_args:{thought.next_tool_args}"
                    user_str=f"observation: {thought.observation}"
                else:
                    if self.thoughts[-1].is_error==None and thought.is_error!=None:
                        assistant_str = (
                            f"next_thought:{thought.next_thought}\n"
                            f"next_tool_name:{thought.next_tool_name}\n"
                            f"next_tool_args:{thought.next_tool_args}")
                        user_str=(
                            f"observation: error ocurred. detailed output omitted "
                            f"({len(thought.observation.splitlines()) if thought.observation is not None else 0}) lines\n"
                        )
                    else:
                        assistant_str=f"next_thought:{thought.next_thought}\nnext_tool_name:{thought.next_tool_name}\nnext_tool_args:{thought.next_tool_args}"
                        user_str=f"observation: {thought.observation}"
            messages.append({"role":"assistant","content":assistant_str})
            messages.append({"role":"user","content":user_str})
        return messages
    
    def export_to_csv(self,file_path:str="./xray.csv"):
        with open(file_path, "w") as f:
            writer=csv.writer(f)
            writer.writerow(["next_thought","next_tool_name","next_tool_args","observation","is_error","raw_response","total_attempts","is_deleted"])
            if len(self.thoughts)>0:
                for thought in self.thoughts:
                    writer.writerow([thought.next_thought,thought.next_tool_name,thought.next_tool_args,thought.observation,thought.is_error,thought.raw_response,thought.total_attempts,str(thought.inference_error_counter),str(thought.request_data),len(str(thought.request_data)),thought.is_deleted])
                
                
    def get_tokens_used(self):
        # quick, safe heuristic assuming ~0.75 tokens/word
        msgs = self.to_str()
        text = "\n".join(m["content"] for m in msgs)
        word_count = len(text.split())
        return int(word_count * 0.75)
    
    
class Utils:
    @classmethod
    def get_available_modules(cls) -> set[str]:
        """Return the set of top-level module names that can be imported in the
        *current* Python environment.

        The result includes:
        • built-in/stdlib module names (`sys.builtin_module_names`)
        • every top-level name discoverable on `sys.path` via `pkgutil.iter_modules()`
        This is useful when we need to check whether a piece of code depends on a
        package that is *not* present in the environment.
        """
        import sys, pkgutil

        available: set[str] = set(sys.builtin_module_names)
        for module_info in pkgutil.iter_modules():
            # Only keep the top-level package name (before the first dot)
            top_level = module_info.name.split(".")[0]
            available.add(top_level)
        return available

    @classmethod
    def message_to_str(cls,messages:list[dict]): 
        final_str=""
        for message in messages:
            role=message["role"]
            content=message["content"]
            final_str+=f"{role}: {content}\n"
        return final_str
    
    @classmethod
    def limit_strings(cls,strings: str, n=1000)->str:
        '''
        Limit the number of strings to 1000
        '''
        strings_list=strings.split("\n")
        if len(strings_list)>n:
            return "\n".join(strings_list[:n])+"\n..." + f"({len(strings_list)-n} more lines)"
        else:
            return strings
    @classmethod
    def load_json(cls,json_string:str)->dict:
        try:
            return json.loads(json_string)
        except Exception as e:
            try:
                return eval(json_string)
            except Exception as e:
                logger.info(f"unable to fix manually, trying with llm")
                fixed_json=Network.fix_json_string_with_llm(json_string)
                if fixed_json:
                    return fixed_json
                else:
                    raise JSONDecodeError(f"Invalid JSON: {json_string}")
    @classmethod
    def log_to_failed_messages(cls,text_resp:str):
        with open("../failed_messages.csv","a") as f:
                writer=csv.writer(f)
                writer.writerow([text_resp])

class Network:
    class ErrorType(Enum):
        EMPTY_RESPONSE=1
        RESERVED_TOKEN_PRESENT=2
        RATE_LIMIT_EXCEEDED=3
        INVALID_RESPONSE_FORMAT=4
        TIMEOUT=5
        UNKNOWN=6
        
    @classmethod
    def is_valid_response(cls,raw_text:str)->bool:
        if type(raw_text) is dict and raw_text.get("error",None) is not None and raw_text.get("error")!="":
            return False,cls.ErrorType.EMPTY_RESPONSE.name
        if len(raw_text)==0:
            return False, cls.ErrorType.EMPTY_RESPONSE.name
        if "<|reserved_token_" in raw_text:
            return False, cls.ErrorType.RESERVED_TOKEN_PRESENT.name
        if 'API request failed with status 429' in raw_text:
            return False, cls.ErrorType.RATE_LIMIT_EXCEEDED.name
        if 'Read timed out' in raw_text:
            return False, cls.ErrorType.TIMEOUT.name
        return True, None
    @classmethod
    def get_error_counter(cls)->dict[str,int]:
        return {
            k:0 for k in cls.ErrorType.__members__
        }   
    @classmethod
    def fix_json_string_with_llm(cls,json_string:str,attempt:int=0)->dict:
        messages=[
            {"role":"system", "content":"Fix the json string sent by the user.  Reply only with the json string and nothing else."},
            {"role":"user", "content":json_string}
        ]
        response=cls.make_request(messages)
        try:
            response=response.replace('```json','').strip('```')
            response=json.loads(response)
            return response
        except JSONDecodeError as e:
            logger.error(f"Error fixing json string: {e},trying again..")
            logger.error(f"json string is :{json_string}")
            logger.error(f"LLM response is :{response}")
            attempt+=1
            if attempt>5:
                return None
            return cls.fix_json_string_with_llm(json_string,attempt)
            
            
    @classmethod
    def make_request(cls,messages:list,attempt:int=0)->str:
        url = f"{DEFAULT_PROXY_URL.rstrip('/')}/agents/inference"
        
        # Cache miss - make the actual request
        request_data = {
                "run_id": run_id if run_id else "1",
                "messages": messages,
                "temperature": 0.0,
            }

        headers = {
            "Content-Type": "application/json"
        }
        request_data['model']=AGENT_MODELS[attempt%len(AGENT_MODELS)]
        response = requests.post(url, json=request_data, timeout=120, headers=headers)
        print(f"[agent] HTTP {response.status_code} from {url} ({len(response.content)} bytes)")
        
        response.raise_for_status()
        response_json = response.json()
        is_oai_interface= type(response_json) is dict and response_json.get('choices') is not None and len(response_json.get('choices'))>0 and response_json.get('choices')[0].get('message') is not None
        if is_oai_interface:
            raw_text=response_json['choices'][0]['message']['content']
        else:
            if type(response_json) is str:
                raw_text=response_json.strip("\n").strip()
            else:
                raw_text=response_json
        if type(raw_text) is not dict:
            raw_text=raw_text.lstrip()
        return raw_text
    
    @classmethod
    def _request_next_action_with_retry(cls, messages: dict, 
                            max_retries: int = 10, 
                            base_delay: float = 2.0) -> str:
        
        raw_text='not defined'
        error_counter=cls.get_error_counter()
        next_thought, next_tool_name, next_tool_args = None, None, None
        total_attempts=0
        for attempt in range(max_retries):
            try:
                total_attempts+=1
                raw_text=cls.make_request(messages,attempt=attempt)
                is_valid,error_msg=cls.is_valid_response(raw_text)
                if not(is_valid):
                    logger.error("--------------------------------")
                    logger.error(f"raw_text: {raw_text}")
                    logger.error("--------------------------------")
                    raise Exception(error_msg)
                    
                next_thought, next_tool_name, next_tool_args,error_msg = cls.parse_response(raw_text)
                if error_msg:
                    raise Exception(error_msg)
                break  # Success, exit retry loop
            except Exception as e:
                error_body = str(e)
                logger.error(f"Error: {error_body}")
                if attempt < max_retries:
                    delay = min(base_delay * (2 ** attempt),8)
                    logger.info(error_body)
                    logger.error("--------------------------------")
                    logger.error(f"response: {raw_text}")
                    logger.error("--------------------------------")
                    logger.info(f"[agent] Retrying in {delay} seconds... (attempt {attempt + 1}/{max_retries})") 
                    if "RATE_LIMIT_EXCEEDED" in error_body:
                        error_counter[cls.ErrorType.RATE_LIMIT_EXCEEDED.name]+=1
                    elif "RESERVED_TOKEN_PRESENT" in error_body:
                        error_counter[cls.ErrorType.RESERVED_TOKEN_PRESENT.name]+=1
                    elif "EMPTY_RESPONSE" in error_body:
                        error_counter[cls.ErrorType.EMPTY_RESPONSE.name]+=1
                    elif "TIMEOUT" in error_body:
                        error_counter[cls.ErrorType.TIMEOUT.name]+=1
                    elif "Invalid JSON" in error_body:
                        error_counter[cls.ErrorType.INVALID_RESPONSE_FORMAT.name]+=1
                    elif "Invalid response" in error_body:
                        error_counter[cls.ErrorType.INVALID_RESPONSE_FORMAT.name]+=1
                    else:
                        error_counter[cls.ErrorType.UNKNOWN.name]+=1
                    if "RATE_LIMIT_EXCEEDED" not in error_body and "RESERVED_TOKEN_PRESENT" not in error_body and "EMPTY_RESPONSE" not in error_body and  "TIMEOUT" not in error_body:
                        messages.append({"role":"assistant","content":raw_text})
                        messages.append({"role":"user","content":"observation: "+error_body})
                    time.sleep(random.uniform(2*delay, 2.2*delay))
                    continue
                else:
                    error_counter[cls.ErrorType.TIMEOUT.name]+=1
                    # Last attempt failed, raise the error
                    raise RuntimeError(error_body)
        
        return next_thought, next_tool_name, next_tool_args,raw_text,total_attempts,error_counter,messages
    
    @classmethod
    def parse_malformed_json(cls,arguments:list[str], json_string:str)->dict | str:    
        # pattern of general json string with unescaped " in values keys from keys list
        pattern = ''
        for i, k in enumerate(arguments):
            pattern += f'"{k}": (.*)'
            if i != len(arguments) - 1:
                pattern += ',\s*'

        match=re.search(pattern, json_string)

        if not match:
            return f"Error: {json_string} can not match pattern {pattern}"
        
        result_json={}
        for i in range(len(arguments)):
            value=match.group(i+1)
            value=value.strip()
            if value.startswith('"') and value.endswith('"'):
                value=value[1:-1]
            #value=value.replace('"', '\\"')
            value=value.replace('\\n','\n')
            result_json[arguments[i]]=value
        return result_json
    
    
    
    @classmethod
    def parse_next_tool_args(cls,tool_name:str, next_tool_args: str)->dict | str:
        '''
        parse string to json, fix unecaped " in values like this: '{"a": "text "text2" text3 "text4"", "b": "text3"}'
        returns json or error message
        '''

        next_tool_args=next_tool_args.replace('```json','').strip('```')
        error_msg=''

        try:
            next_tool_args = Utils.load_json(next_tool_args.strip())
        except JSONDecodeError as e:
            error_msg=f"Invalid JSON: {next_tool_args}"    
            try:
                next_tool_args = cls.parse_malformed_json(ToolManager.get_tool_args_for_tool(tool_name,required=True), next_tool_args)
            except ToolManager.Error as e:
                raise Exception(e.message)
            except Exception as e:
                raise Exception(error_msg)
        return next_tool_args
    
    @classmethod
    def inference(cls, messages: List[Dict[str, Any]], run_id: str = "1",return_json:bool=False) -> dict:
        """Prod inference with caching"""
        # Build request data
        cleaned_msgs: List[Dict[str, Any]] = []
        for m in messages:
            role = m.get("role")
            if role not in {"system", "user", "assistant", "tool"}:
                continue  # skip anything non-standard
            content = m.get("content", "")

            # Ignore assistant placeholders that only carry the internal
            # ``tool_call`` and have no visible content.
            if role == "assistant" and not content.strip():
                continue

            cleaned_msgs.append({"role": role, "content": content})

        if not cleaned_msgs:
            raise RuntimeError("No valid messages to send to proxy.")

        next_thought,next_tool_name,next_tool_args,raw_text,total_attempts,error_counter,messages = cls._request_next_action_with_retry(cleaned_msgs)
        
        return next_thought,next_tool_name,next_tool_args,raw_text,total_attempts,error_counter,messages
    
    @classmethod
    def sanitise_text_resp(cls,text_resp:str)->str:
        # remove all leading and trailing quotes
        text_resp=re.sub("[\'\"]*next_thought[\'\"]*:","next_thought:",text_resp)
        text_resp=re.sub("[\'\"]*next_tool_name[\'\"]*:","next_tool_name:",text_resp)
        text_resp=re.sub("[\'\"]*next_tool_args[\'\"]*:","next_tool_args:",text_resp)
        text_resp=re.sub("[\'\"]*observation[\'\"]*:","observation:",text_resp)
        if "next_thought" not in text_resp and "next_tool_name:" in text_resp and "next_tool_args:" in text_resp and text_resp.find("next_tool_name:")<text_resp.find("next_tool_args:") and text_resp.find("next_tool_name:")>10:
            logger.info(f"next_thought not found in {text_resp[:50]}, adding it")
            text_resp="next_thought: "+text_resp
        if "next_tool_name:" in text_resp and "next_tool_args:" in text_resp and text_resp.find("next_tool_name:")<text_resp.find("next_tool_args:"):
            # remove all leading and trailing quotes in tool_name
            next_tool_name=text_resp.split("next_tool_name:")[1].split("next_tool_args:")[0].strip().strip("\n").strip("\'").strip("\"").strip()
            print(text_resp)
            text_resp=re.sub(f"next_tool_name:[\'\" ]*{next_tool_name}[\'\" ]*","next_tool_name: "+next_tool_name,text_resp)
        
        return text_resp
    
    @classmethod
    def parse_response(cls,text_resp: str)->tuple[str, str, dict]:
        error_msg=None
        text_resp = text_resp.strip()
        text_resp=text_resp.split("observation:")[0]
        text_resp=text_resp.strip().strip("\n")
        text_resp=cls.sanitise_text_resp(text_resp)
        if "next_thought:" in text_resp and "next_tool_name:" in text_resp and "next_tool_args:" in text_resp and text_resp.find("next_thought:")<text_resp.find("next_tool_name:") and text_resp.find("next_tool_name:")<text_resp.find("next_tool_args:"):
            next_thought=text_resp.split("next_thought:")[1].split("next_tool_name:")[0].strip().strip("\n")
            next_tool_name=text_resp.split("next_tool_name:")[1].split("next_tool_args:")[0].strip().strip("\n")
            next_tool_args=text_resp.split("next_tool_args:")[1].strip().split("next_thought:")[0].strip().strip("\n")
            try:
                next_tool_args=cls.parse_next_tool_args(next_tool_name, next_tool_args)
            except JSONDecodeError as e:
                error_msg=f"Invalid JSON: {str(e)}"
                Utils.log_to_failed_messages(text_resp)
                
        else:
            if "next_thought:" not in text_resp:
                error_msg="Invalid response. next_thought not found"
            elif "next_tool_name:" not in text_resp:
                error_msg="Invalid response. next_tool_name not found"
            elif "next_tool_args:" not in text_resp:
                error_msg="Invalid response. next_tool_args not found"
            elif text_resp.find("next_thought:")>text_resp.find("next_tool_name:"):
                error_msg="Invalid response. next_thought is after next_tool_name"
            elif text_resp.find("next_tool_name:")>text_resp.find("next_tool_args:"):
                error_msg="Invalid response. next_tool_name is after next_tool_args"
            else:
                logger.error(f"We have no clue why parsing failed. Please check this \n{text_resp}\n")
                error_msg=f"Invalid response. Please follow the response format {FORMAT_PROMPT}"
            Utils.log_to_failed_messages(text_resp)
            return None,None,None,error_msg

        return next_thought, next_tool_name, next_tool_args,error_msg

class FunctionVisitor(ast.NodeVisitor):
    def __init__(self, file_content: str):
        self.functions = {}
        self.current_class = None
        self.class_hierarchy = []
        self.file_content = file_content

    def visit_ClassDef(self, node):
        self.class_hierarchy.append(node.name)
        self.current_class = "::".join(self.class_hierarchy)
        self.generic_visit(node)
        self.class_hierarchy.pop()
        self.current_class = "::".join(self.class_hierarchy) if self.class_hierarchy else None

    def _process_function(self, node):
        full_function_name = f"{self.current_class}::{node.name}" if self.current_class else node.name
        line_number = node.lineno
        if isinstance(node.decorator_list, list) and len(node.decorator_list) > 0:
            line_number = node.decorator_list[0].lineno
        
        end_line_number = line_number
        if isinstance(node.body, list) and len(node.body) > 0:
            end_line_number = node.body[-1].lineno
        
        lines = self.file_content.split("\n")
        body = "\n".join(lines[line_number-1:end_line_number])
        
        self.functions[full_function_name] = {
            "class": self.current_class,
            "body": body,
            "line_number": line_number
        }
        self.generic_visit(node)

    def visit_FunctionDef(self, node):
        self._process_function(node)

    def visit_AsyncFunctionDef(self, node):
        self._process_function(node)

    def visit_Module(self, node):
        self.current_class = None
        self.generic_visit(node)
        self.current_class = None

class ToolManager:
    TOOL_LIST={}
    
    # decorator used to mark instance methods as "tools"
    def tool(fn):
        def wrapper(self, *args, **kwargs):
            self.tool_invocations[fn.__name__]+=1
            try:
                return fn(self, *args, **kwargs)
            except ToolManager.Error as e:
                self.tool_failure[fn.__name__][e.error_type]+=1
                return e.message

        # Preserve original function metadata
       
        wrapper.__name__ = fn.__name__
        wrapper.__doc__ = fn.__doc__
        wrapper.__signature__ = inspect.signature(fn)
        wrapper.__annotations__ = fn.__annotations__.copy()
        wrapper.is_tool=True

        return wrapper
    
    class Error(Exception):
        class ErrorType(Enum):
            SYNTAX_ERROR=1
            RUNTIME_ERROR=2
            TIMEOUT=3
            FILE_NOT_FOUND=4
            SEARCH_TERM_NOT_FOUND=5
            UNKNOWN=6
            THIRD_PARTY_DEPENDENCIES=7
            MULTIPLE_SEARCH_RESULTS_FOUND=8
            BUG_REPORT_REQUIRED=9
            INVALID_RESPONSE_FORMAT=10
            INVALID_TOOL_NAME=11
            INVALID_FILE_PATH=12
            INVALID_TOOL_CALL=13
            IMPORT_ERROR=14
            GIT_OPERATION_FAILED=15
            GIT_CONFIG_ERROR=16
            GIT_STATE_ERROR=17
            GIT_MERGE_CONFLICT=18
            GIT_BRANCH_ERROR=19
            TEST_COVERAGE_ERROR = 20
            DEPENDENCY_ANALYSIS_ERROR = 21
            CODE_SMELL_DETECTION_ERROR = 22
            GIT_HISTORY_ERROR = 23
            CODE_QUALITY_ERROR = 24
            SOLUTION_VALIDATION_ERROR = 25
            CODE_STYLE_ERROR = 26
            SOLUTION_COMPARISON_ERROR = 27
            
        def __init__(self,error_type:ErrorType,message:str):    
            self.error_type=error_type
            self.message=message

    def __init__(self, available_tools: Optional[list[str]] = None):
        self.new_files_created=[]
        self.is_solution_approved=False
        # Initialize parallel execution components
        self.performance_monitor = PerformanceMonitor()
        self.parallel_executor = ParallelToolExecutor(self)
        self.file_searcher = ParallelFileSearcher(self)
        self.file_processor = ParallelFileProcessor(self)
        self.dependency_executor = DependencyAwareParallelExecutor(self)
        
        for name, attr in self.__class__.__dict__.items():
            if getattr(attr, "is_tool", False) and name not in ToolManager.TOOL_LIST:
                if available_tools is not None and name not in available_tools: # if available_tools is provided, only include tools in the list
                    continue
                ToolManager.TOOL_LIST[name] = self.__class__.tool_parsing(attr)
        logger.info(f"Tool list: {chr(10).join(list(ToolManager.TOOL_LIST.keys()))}")
        self.tool_failure={
            k:{j:0 for j in self.Error.ErrorType.__members__} for k in self.TOOL_LIST.keys()
        }
        self.tool_invocations={
          k:0 for k in self.TOOL_LIST.keys()
        }
        
    def check_syntax_error(self,content:str,file_path:str="<unknown>")->bool:
        try:
            ast.parse(content, filename=file_path)
            return False, None
        except SyntaxError as e:
            logger.error(f"Syntax error: {e}")
            return True, ToolManager.Error(ToolManager.Error.ErrorType.SYNTAX_ERROR.name,f"Syntax error. {str(e)}")
        
    @classmethod
    def tool_parsing(cls,fn):
        tool_schemas = None
        name = fn.__name__
        doc_fn = fn.__doc__ or ""
        # remove parameters section from here to be put in args section
        doc=doc_fn.split("Arguments:")[0]
        output_description=doc_fn.split("Output:")
        if len(output_description)>1:
            output_description="Output: "+output_description[1].strip()
            doc=doc+"\n\n"+output_description
        sig = inspect.signature(fn)
        properties = {}
        required = []
        for param in sig.parameters.values():
            if param.name == 'self':
                continue
            if param.default is param.empty and param.kind in (param.POSITIONAL_OR_KEYWORD, param.KEYWORD_ONLY):
                required.append(param.name)
            type_hint = str(param.annotation) if param.annotation != param.empty else "string"
            param_description=re.search(f"{param.name}:([^\n]+)",doc_fn)
            if param_description:
                param_description=param_description.group(1)
            else:
                raise ValueError(f"Parameter description not found for {param.name} in {doc_fn}: tool name: {name}")
            # Special handling for list[str] / List[str] annotations so that the
            # generated JSON schema correctly represents an array of strings.
            if ("list" in type_hint.lower()) and ("str" in type_hint):
                properties[param.name] = {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": param_description
                }
                continue
            elif 'str' in type_hint:
                json_type = "string"
            elif 'int' in type_hint:
                json_type = "integer"
            elif 'float' in type_hint:
                json_type = "number"
            elif 'bool' in type_hint:
                json_type = "boolean"
            else:
                json_type = "string"
            properties[param.name] = {
                "type": json_type,
                "description": param_description
            }
        parameters = {
            "type": "object",
            "properties": properties,
            "required": required
        }
        tool_schemas={
            "name": name,
            "description": doc.strip(),
            "input_schema": parameters
        }
        
        return tool_schemas
    
    
    @classmethod
    def get_tool_docs(cls)->str:
        return '\n\n'.join([json.dumps(tool_metadata, ensure_ascii=False) for _,tool_metadata in cls.TOOL_LIST.items()])
    
    def get_tool(self,tool_name:str):
        if tool_name not in self.TOOL_LIST:
            raise ToolManager.Error(ToolManager.Error.ErrorType.INVALID_TOOL_NAME.name,f"Error: tool '{tool_name}' not found")
        tool_method = getattr(self, tool_name, None)
        if tool_method is None or not callable(tool_method):
            raise ToolManager.Error(
                ToolManager.Error.ErrorType.INVALID_TOOL_NAME.name,
                f"Error: tool '{tool_name}' does not exist. Please use one of the following tools: {', '.join(self.TOOL_LIST.keys())}"
            )

        return tool_method
    
    
    def _get_file_content(
        self,
        file_path: str,
        search_start_line: int = None,
        search_end_line: int = None,
        search_term: str = None,
        limit: int = 5000
    ) -> str:
        """
        Retrieve file content, optionally limited to a line range or matching a search term.

        - If search_term is provided, ignores line ranges and returns search results.
        - If line range is provided, adjusts to function boundaries.
        - If limit != -1, trims output to n characters.
        """

        # If search term is provided, use specialized search
        if search_term:
            logger.debug(f"search_term specified: {search_term}, searching in v2")
            return self.search_in_specified_file_v2(file_path, search_term)

        # Adjust start/end lines if they fall within a function
        func_ranges = self.get_function_ranges(file_path)

        if search_start_line is not None:
            for start, end, name in func_ranges:
                if start <= search_start_line <= end and start < search_start_line:
                    logger.debug(f"Adjusting start line {search_start_line} to {start} (function {name})")
                    search_start_line = start

        if search_end_line is not None:
            for start, end, name in func_ranges:
                if start <= search_end_line <= end and end > search_end_line:
                    logger.debug(f"Adjusting end line {search_end_line} to {end} (function {name})")
                    search_end_line = end

        logger.debug(f"search start line: {search_start_line}, search end line: {search_end_line}")

        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            if search_start_line is not None or search_end_line is not None:
                lines = f.readlines()
                start_idx = max(0, (search_start_line or 1) - 1)
                end_idx = min(len(lines), search_end_line or len(lines))
                content = "".join(lines[start_idx:end_idx])
                return f"Lines {start_idx+1}-{end_idx} of {file_path}:\n{content}"
            else:
                content = f.read()

        return Utils.limit_strings(content, n=limit) if limit != -1 else content

    
    @tool
    def get_file_content(self,file_path: str, search_start_line: int = None, search_end_line: int = None, search_term: str = None)->str:
       
        '''
        Retrieves file contents with optional filtering based on search term and line numbers
        Arguments:
            file_path: filesystem path to target file. This file must be python file.
            search_start_line: optional start line number to begin extraction (1-indexed)
            search_end_line: optional end line number to end extraction (1-indexed)
            search_term: optional text pattern to filter matching lines
        '''
        return self._get_file_content(file_path,search_start_line,search_end_line,search_term,limit=5000)
    
    @tool
    def analyze_test_coverage(self, test_func_names: List[str]) -> str:
        '''
        Analyze test coverage for proposed test functions
        Arguments:
            test_func_names: List of test function names with file paths
        Output:
            Coverage analysis report showing which code paths are tested
        '''
        try:
            # Use coverage.py to analyze test coverage
            result = subprocess.run(["coverage", "run", "--source=.", "-m", "pytest", "-v", "-k"] + test_func_names, 
                                   capture_output=True, text=True, check=True)
            
            coverage_report = subprocess.run(["coverage", "report", "--format=json"], 
                                            capture_output=True, text=True, check=True)
            
            return coverage_report.stdout
        except Exception as e:
            raise ToolManager.Error(ToolManager.Error.ErrorType.TEST_COVERAGE_ERROR.name, 
                                  f"Test coverage analysis failed: {e}")
    
    @tool
    def analyze_dependencies(self, file_path: str) -> str:
        '''
        Analyze dependencies of a file to understand impact of changes
        Arguments:
            file_path: Path to the file to analyze
        Output:
            List of dependencies and dependent files
        '''
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            dependencies = {
                'imports': [],
                'exporters': [],
                'callers': []
            }
            
            # Find imports
            for node in ast.walk(ast.parse(content)):
                if isinstance(node, (ast.Import, ast.ImportFrom)):
                    dependencies['imports'].append(node.module if isinstance(node, ast.Import) else node.module)
            
            # Find files that import this file
            for root, _, files in os.walk("."):
                for file in files:
                    if file.endswith('.py'):
                        with open(os.path.join(root, file), 'r', encoding='utf-8') as f:
                            if f"import {os.path.basename(file_path).split('.')[0]}" in f.read():
                                dependencies['exporters'].append(os.path.join(root, file))
            
            return json.dumps(dependencies, indent=2)
        except Exception as e:
            raise ToolManager.Error(ToolManager.Error.ErrorType.DEPENDENCY_ANALYSIS_ERROR.name, 
                                  f"Dependency analysis failed: {e}")
            
    @tool
    def analyze_git_history(self, file_path: str, commit_range: str = "HEAD~5..HEAD") -> str:
        '''
        Analyze git history for a file to understand previous changes
        Arguments:
            file_path: Path to the file to analyze
            commit_range: Commit range to analyze (default: last 5 commits)
        Output:
            Git history analysis with commit messages and changes
        '''
        try:
            result = subprocess.run(["git", "log", commit_range, "--pretty=format:%H%n%an%n%ad%n%s%n%b", "--", file_path],
                                  capture_output=True, text=True, check=True)
            commits = result.stdout.split("\n\n")
            analysis = []
            
            for commit in commits:
                lines = commit.split("\n")
                if len(lines) >= 4:
                    analysis.append(f"Commit: {lines[0]}")
                    analysis.append(f"Author: {lines[1]}")
                    analysis.append(f"Date: {lines[2]}")
                    analysis.append(f"Message: {lines[3]}")
                    analysis.append("-" * 50)
            
            return "\n".join(analysis) if analysis else "No git history found for this file"
        except Exception as e:
            raise ToolManager.Error(ToolManager.Error.ErrorType.GIT_HISTORY_ERROR.name, 
                                  f"Git history analysis failed: {e}")
    
    @tool
    def get_code_quality_metrics(self, file_path: str) -> str:
        '''
        Calculate code quality metrics for a file
        Arguments:
            file_path: Path to the file to analyze
        Output:
            Code quality metrics including cyclomatic complexity, maintainability index, etc.
        '''
        try:
            # Use radon for code complexity analysis
            result = subprocess.run(["radon", "cc", "-s", file_path], 
                                  capture_output=True, text=True, check=True)
            
            metrics = {
                "cyclomatic_complexity": result.stdout,
                "maintainability_index": "N/A",
                "halstead_metrics": "N/A"
            }
            
            # Add maintainability index analysis if needed
            # Add halstead metrics analysis if needed
            
            return json.dumps(metrics, indent=2)
        except Exception as e:
            raise ToolManager.Error(ToolManager.Error.ErrorType.CODE_QUALITY_ERROR.name, 
                                  f"Code quality metrics failed: {e}")
    
    @tool
    def validate_solution(self, file_path: str, test_func_names: List[str]) -> str:
        '''
        Validate a proposed solution against all test functions
        Arguments:
            file_path: Path to the file with the proposed solution
            test_func_names: List of test functions to validate against
        Output:
            Validation results showing which tests pass/fail
        '''
        try:
            # Run tests against the specific file
            result = subprocess.run(["python", "-m", "pytest", "-v", "-k"] + test_func_names, 
                                  capture_output=True, text=True, check=True)
            
            # Parse test results
            test_results = []
            for line in result.stdout.splitlines():
                if "FAIL" in line or "ERROR" in line:
                    test_results.append(f"❌ {line}")
                elif "PASS" in line:
                    test_results.append(f"✅ {line}")
            
            return "\n".join(test_results) if test_results else "No test results found"
        except Exception as e:
            raise ToolManager.Error(ToolManager.Error.ErrorType.SOLUTION_VALIDATION_ERROR.name, 
                                  f"Solution validation failed: {e}")
    
    
    @tool
    def compare_solutions(self, solution1: str, solution2: str) -> str:
        '''
        Compare two proposed solutions for pros/cons
        Arguments:
            solution1: First solution to compare
            solution2: Second solution to compare
        Output:
            Comparison analysis of the two solutions
        '''
        try:
            # Use LLM to compare solutions
            comparison_prompt = f"Compare these two solutions for the problem:\n\nSolution 1:\n{solution1}\n\nSolution 2:\n{solution2}\n\n"
            comparison_prompt += "Analyze pros/cons of each solution in terms of:\n"
            comparison_prompt += "- Code readability\n- Performance impact\n- Test coverage\n- Backward compatibility\n- Maintainability"
            
            return self.llm_complete(comparison_prompt)
        except Exception as e:
            raise ToolManager.Error(ToolManager.Error.ErrorType.SOLUTION_COMPARISON_ERROR.name, 
                                  f"Solution comparison failed: {e}")
    
    @tool        
    def detect_code_smells(self, file_path: str) -> str:
        '''
        Detect code smells and anti-patterns in a file
        Arguments:
            file_path: Path to the file to analyze
        Output:
            List of code smells with line numbers and suggestions
        '''
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            smells = []
            
            # Detect long functions
            tree = ast.parse(content)
            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    if node.body and len(node.body) > 20:  # Arbitrary threshold
                        smells.append(f"Long function: {node.name} (lines {node.lineno}-{node.end_lineno})")
            
            # Detect magic numbers
            for line_num, line in enumerate(content.splitlines(), 1):
                if re.search(r'\b\d+\b', line):
                    smells.append(f"Magic number detected on line {line_num}: {line.strip()}")
            
            # Detect duplicated code
            if "duplicate" in content.lower():
                smells.append("Potential code duplication detected")
            
            return "\n".join(smells) if smells else "No code smells detected"
        except Exception as e:
            raise ToolManager.Error(ToolManager.Error.ErrorType.CODE_SMELL_DETECTION_ERROR.name, 
                                  f"Code smell detection failed: {e}")
    
    def save_file(self,file_path: str, content: str)->str:
        '''
        Writes text content to specified filesystem location. If there are any syntax errors in the code, it rejects the edit with an error message. Do not use this tool to create test or files to reproduce the error.
        Arguments:
            file_path: target filesystem path
            content: text data to write
        '''
        if "test" in file_path.lower() or "reproduce" in file_path.lower():
            raise ToolManager.Error(ToolManager.Error.ErrorType.INVALID_TOOL_CALL.name,f"Error: You cannot use this tool to create test or files to reproduce the error.")
        return self._save(file_path, content)
    
    @tool   
    def get_approval_for_solution(self, solutions: list[str], selected_solution: int, reason_for_selection: str) -> str:
        '''
        This tool is used to get approval for your proposed solution. You need to propose at least 2 meaningfully different and elegant solutions to the problem.
        While all the solutions proposed need to be accurate, the following are guidelines for selecting the best solution:
        1. Expected output should be closest to the most relevant test case.
        Arguments:
            solutions: list of solutions proposed by you. Each solution should be very detailed and explain why it is better than the other solutions.
            selected_solution: Index of the solution you think is the best.
            reason_for_selection: Reason for selecting the solution over other solutions.
            
        Output:
            approval: approved/not approved. If approved, you can go ahead and implement the solution.
        '''
        logger.info(f"solutions: {solutions}")
        logger.info(f"selected_solution: {selected_solution}")
        logger.info(f"reason_for_selection: {reason_for_selection}")
        
        parsed_solutions = []
        for solution in solutions:
            sols = re.split(r"(Solution \d+:)", solution)
            sols = [f"{sols[i]}{sols[i+1]}" for i in range(1, len(sols), 2)]  # Combine the split parts correctly
            parsed_solutions.extend(sols)
        
        solutions = parsed_solutions
        # if type(solutions) is not list or len(solutions) < 2:
        #     raise ToolManager.Error(ToolManager.Error.ErrorType.INVALID_TOOL_CALL.name, f"Error: solutions must be a list with length at least 2.")
        
        self.is_solution_approved = True
        return "Approved"
    
    def _search_in_file(self, file_path: str, search_term: str)->str:
        '''
        Search for a term in a file
        '''
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
            
            if search_term.lower() not in content.lower():
                return []

            # Parse the file content using AST
            tree = ast.parse(content, filename=file_path)
            visitor = FunctionVisitor(content)
            visitor.visit(tree)

            output = []
            for function_name, function_info in visitor.functions.items():
                body = function_info["body"]
                if search_term.lower() in body.lower():
                    # split body into lines
                    lines = body.split("\n")
                    for idx, line in enumerate(lines):
                        if search_term.lower() in line.lower():
                            line_number = function_info["line_number"] + idx
                            output.append(f"{file_path}:{line_number} | {function_name} | {line.rstrip()}")
        except Exception as e:
            logger.error(f"Error searching in file {file_path} with search term {search_term}: {e}")
            return []
        
        return output

    def _save(self,file_path: str, content: str)->str:
        is_syntax_error, error = self.check_syntax_error(content)
        if not is_syntax_error:
            with open(file_path, "w") as file:
                file.write(content)
            self.new_files_created.append(file_path)
            return f"File {file_path} saved successfully"
        else:
            logger.error(f"Error saving file: {error.message}")
            error.message="Error saving file. "+error.message
            raise ToolManager.Error(ToolManager.Error.ErrorType.SYNTAX_ERROR.name,error.message)
    
    @tool
    def get_function_body(self, file_path: str, function_name: str) -> str:
        """
        Extract the body/source code of a specific function from a file.
        Args:
            file_path: Path to the Python file
            function_name: Name of the function to extract
        Returns:
            The full source code of the function
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
        except Exception as e:
            raise ToolManager.Error(ToolManager.Error.ErrorType.FILE_NOT_FOUND.name, f"Error reading '{file_path}': {e}")

        try:
            tree = ast.parse(content, filename=file_path)
        except SyntaxError as e:
            raise ToolManager.Error(ToolManager.Error.ErrorType.SYNTAX_ERROR.name, f"Error parsing '{file_path}': {e}")

        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                if node.name == function_name:
                    # Include decorators in the start line if they exist
                    start_line = node.lineno
                    if node.decorator_list:
                        start_line = node.decorator_list[0].lineno

                    # Use ast.get_source_segment if available (Python 3.8+)
                    if hasattr(ast, 'get_source_segment'):
                        source = ast.get_source_segment(content, node)
                        if source:
                            return source
                    
                    # Fallback: manual source extraction
                    end_line = getattr(node, 'end_lineno', None)
                    if end_line is None:
                        # Find the end line by checking all child nodes
                        end_line = start_line
                        for child in ast.walk(node):
                            if hasattr(child, 'lineno'):
                                end_line = max(end_line, child.lineno)
                    
                    lines = content.splitlines()
                    return "\n".join(lines[start_line - 1:end_line])

        raise ToolManager.Error(ToolManager.Error.ErrorType.SEARCH_TERM_NOT_FOUND.name, f"Function '{function_name}' not found in '{file_path}'")

    @tool
    def search_in_all_files_content_v2(self, grep_search_command: str, test_files_only: bool = False) -> str:
        '''
        Performs grep search across all files in the codebase
        Arguments:
            grep_search_command: grep search command to locate (e.g., "grep -rn --include='*.py' . -e 'db.*passwd\|passwd.*db'). if test_files_only is True, then add --include='test_*.py' --include='*_test.py' --include='*test*.py' to the command.
            test_files_only: if True, search only in test files; if False, search all files
        Output:
            locations where pattern was found with file paths and line numbers
        '''
        output = subprocess.run(["bash", "-c", grep_search_command], capture_output=True)
        
        output = output.stdout.decode("utf-8")
        output = Utils.limit_strings(output, n=100)
        if not output:
            file_type = "test files" if test_files_only else "the codebase"
            raise ToolManager.Error(ToolManager.Error.ErrorType.SEARCH_TERM_NOT_FOUND.name, f"'{grep_search_command}' not found in {file_type}.")
        return output

    @tool
    def get_git_status(self) -> str:
        '''
        Get the current git status of the repository
        Arguments:
            None
        Output:
            Current git status including branch, staged/unstaged changes, and untracked files
        '''
        try:
            result = subprocess.run(["git", "status"], capture_output=True, text=True, check=True)
            return result.stdout
        except subprocess.CalledProcessError as e:
            raise ToolManager.Error(ToolManager.Error.ErrorType.RUNTIME_ERROR.name, f"Git status failed: {e.stderr}")

    @tool
    def get_git_log(self, num_commits: int = 10) -> str:
        '''
        Get recent git commit history
        Arguments:
            num_commits: Number of recent commits to show (default: 10)
        Output:
            Recent commit history with commit hashes, authors, dates, and messages
        '''
        try:
            result = subprocess.run(["git", "log", f"-{num_commits}", "--oneline", "--graph"], capture_output=True, text=True, check=True)
            return result.stdout
        except subprocess.CalledProcessError as e:
            raise ToolManager.Error(ToolManager.Error.ErrorType.RUNTIME_ERROR.name, f"Git log failed: {e.stderr}")

    @tool
    def get_git_branches(self) -> str:
        '''
        Get all git branches in the repository
        Arguments:
            None
        Output:
            List of all branches with current branch marked
        '''
        try:
            result = subprocess.run(["git", "branch", "-a"], capture_output=True, text=True, check=True)
            return result.stdout
        except subprocess.CalledProcessError as e:
            raise ToolManager.Error(ToolManager.Error.ErrorType.RUNTIME_ERROR.name, f"Git branch failed: {e.stderr}")

    @tool
    def get_git_diff(self, file_path: str = None) -> str:
        '''
        Get git diff for staged/unstaged changes
        Arguments:
            file_path: Optional specific file to get diff for
        Output:
            Git diff showing changes in the repository
        '''
        try:
            if file_path:
                result = subprocess.run(["git", "diff", file_path], capture_output=True, text=True, check=True)
            else:
                result = subprocess.run(["git", "diff"], capture_output=True, text=True, check=True)
            return result.stdout if result.stdout else "No changes detected"
        except subprocess.CalledProcessError as e:
            raise ToolManager.Error(ToolManager.Error.ErrorType.RUNTIME_ERROR.name, f"Git diff failed: {e.stderr}")

    @tool
    def search_git_related_code(self, search_terms: List[str]) -> str:
        '''
        Search for git-related code patterns in the codebase
        Arguments:
            search_terms: List of git-related terms to search for (e.g., ["git", "commit", "merge", "branch"])
        Output:
            Locations where git-related code patterns were found
        '''
        results = []
        for term in search_terms:
            try:
                # Search for the term in Python files
                cmd = f"grep -rn --include='*.py' . -e '{term}'"
                result = subprocess.run(["bash", "-c", cmd], capture_output=True, text=True)
                if result.stdout:
                    results.append(f"=== Search for '{term}' ===\n{result.stdout}")
            except Exception as e:
                results.append(f"Error searching for '{term}': {e}")
        
        if not results:
            raise ToolManager.Error(ToolManager.Error.ErrorType.SEARCH_TERM_NOT_FOUND.name, f"No git-related terms found: {search_terms}")
        
        return Utils.limit_strings("\n".join(results), n=200)

    @tool
    def analyze_git_operations(self, file_path: str) -> str:
        '''
        Analyze a file for git-related operations and patterns
        Arguments:
            file_path: Path to the file to analyze
        Output:
            Analysis of git-related operations found in the file
        '''
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            git_patterns = {
                'subprocess calls': re.findall(r'subprocess\.(?:run|call|Popen).*?git', content, re.IGNORECASE),
                'git imports': re.findall(r'import.*git|from.*git', content, re.IGNORECASE),
                'git commands': re.findall(r'git\s+\w+', content, re.IGNORECASE),
                'repository operations': re.findall(r'repo|repository|commit|merge|branch|checkout', content, re.IGNORECASE),
                'git config': re.findall(r'git\s+config', content, re.IGNORECASE),
                'git status checks': re.findall(r'git\s+status', content, re.IGNORECASE),
                'git log operations': re.findall(r'git\s+log', content, re.IGNORECASE),
                'git diff operations': re.findall(r'git\s+diff', content, re.IGNORECASE),
            }
            
            analysis = f"Git Operations Analysis for {file_path}:\n\n"
            for pattern_type, matches in git_patterns.items():
                if matches:
                    analysis += f"{pattern_type.title()}:\n"
                    for match in matches[:5]:  # Limit to first 5 matches
                        analysis += f"  - {match.strip()}\n"
                    if len(matches) > 5:
                        analysis += f"  ... and {len(matches) - 5} more\n"
                    analysis += "\n"
            
            if not any(git_patterns.values()):
                analysis += "No git-related operations found in this file."
            
            return analysis
            
        except Exception as e:
            raise ToolManager.Error(ToolManager.Error.ErrorType.FILE_NOT_FOUND.name, f"Error analyzing file {file_path}: {e}")

    @tool
    def check_git_workflow_issues(self) -> str:
        '''
        Check for common git workflow issues in the codebase
        Arguments:
            None
        Output:
            Analysis of potential git workflow issues and recommendations
        '''
        issues = []
        
        # Check for hardcoded git commands
        try:
            result = subprocess.run(["grep", "-rn", "--include='*.py'", ".", "-e", "git\\s+[a-z]+"], capture_output=True, text=True)
            if result.stdout:
                issues.append("Found hardcoded git commands in code")
        except:
            pass
        
        # Check for git configuration issues
        try:
            result = subprocess.run(["git", "config", "--list"], capture_output=True, text=True)
            if "user.name" not in result.stdout or "user.email" not in result.stdout:
                issues.append("Git user configuration may be incomplete")
        except:
            issues.append("Unable to check git configuration")
        
        # Check for merge conflict markers
        try:
            result = subprocess.run(["grep", "-rn", "--include='*.py'", ".", "-e", "<<<<<<<|=======|>>>>>>>"], capture_output=True, text=True)
            if result.stdout:
                issues.append("Found merge conflict markers in code")
        except:
            pass
        
        # Check for proper error handling in git operations
        try:
            result = subprocess.run(["grep", "-rn", "--include='*.py'", ".", "-e", "subprocess.*git"], capture_output=True, text=True)
            if result.stdout:
                git_ops = result.stdout.split('\n')
                for op in git_ops:
                    if op and 'check=True' not in op and 'CalledProcessError' not in op:
                        issues.append("Git operations may lack proper error handling")
                        break
        except:
            pass
        
        if not issues:
            return "No obvious git workflow issues detected. Repository appears to follow good practices."
        else:
            return "Potential git workflow issues found:\n" + "\n".join(f"- {issue}" for issue in issues)

    @tool
    def validate_git_solution(self, file_path: str, git_operation: str) -> str:
        '''
        Validate that a git-related fix is working correctly
        Arguments:
            file_path: Path to the file containing the git operation fix
            git_operation: Description of the git operation being tested
        Output:
            Validation results and recommendations for the git solution
        '''
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            validation_results = []
            
            # Check for proper error handling
            if 'subprocess' in content and 'git' in content:
                if 'try:' in content and 'except' in content:
                    validation_results.append("✅ Proper error handling with try-catch blocks")
                else:
                    validation_results.append("❌ Missing error handling for git operations")
            
            # Check for git command validation
            if 'git' in content:
                if 'check=True' in content or 'CalledProcessError' in content:
                    validation_results.append("✅ Git command execution with proper error checking")
                else:
                    validation_results.append("❌ Git commands may not handle errors properly")
            
            # Check for repository state validation
            if any(term in content.lower() for term in ['status', 'branch', 'commit']):
                validation_results.append("✅ Repository state validation present")
            
            # Check for safe git operations
            if any(term in content.lower() for term in ['checkout', 'merge', 'reset']):
                if 'safe' in content.lower() or 'validate' in content.lower():
                    validation_results.append("✅ Safe git operations with validation")
                else:
                    validation_results.append("⚠️ Git operations may need additional safety checks")
            
            # Check for logging
            if 'logger' in content or 'print' in content:
                validation_results.append("✅ Logging present for debugging")
            else:
                validation_results.append("⚠️ Consider adding logging for git operations")
            
            # Check for configuration validation
            if 'config' in content.lower():
                validation_results.append("✅ Git configuration handling present")
            
            if not validation_results:
                validation_results.append("ℹ️ No specific git validation patterns found")
            
            result = f"Git Solution Validation for {file_path}:\n"
            result += f"Operation: {git_operation}\n\n"
            result += "\n".join(validation_results)
            
            return result
            
        except Exception as e:
            raise ToolManager.Error(ToolManager.Error.ErrorType.FILE_NOT_FOUND.name, f"Error validating git solution: {e}")

    @tool
    def test_git_operation(self, git_command: str, expected_output: str = None) -> str:
        '''
        Test a specific git operation to verify it works correctly
        Arguments:
            git_command: The git command to test (e.g., "git status", "git log --oneline")
            expected_output: Optional expected output pattern to verify
        Output:
            Result of the git operation and whether it matches expectations
        '''
        try:
            # Split the command into parts for subprocess
            cmd_parts = git_command.split()
            if cmd_parts[0] != 'git':
                raise ToolManager.Error(ToolManager.Error.ErrorType.INVALID_TOOL_CALL.name, "Command must start with 'git'")
            
            result = subprocess.run(cmd_parts, capture_output=True, text=True, check=True)
            
            output = f"Command: {git_command}\n"
            output += f"Exit code: {result.returncode}\n"
            output += f"Output:\n{result.stdout}\n"
            
            if result.stderr:
                output += f"Stderr:\n{result.stderr}\n"
            
            if expected_output:
                if expected_output.lower() in result.stdout.lower():
                    output += f"✅ Expected output pattern '{expected_output}' found in result"
                else:
                    output += f"❌ Expected output pattern '{expected_output}' not found in result"
            
            return output
            
        except subprocess.CalledProcessError as e:
            return f"Git command failed:\nCommand: {git_command}\nExit code: {e.returncode}\nError: {e.stderr}"
        except Exception as e:
            raise ToolManager.Error(ToolManager.Error.ErrorType.RUNTIME_ERROR.name, f"Error testing git operation: {e}")

    @tool
    def search_in_all_files_content(self,search_term: str)->str:
        '''
        Performs text pattern matching across all files in the codebase
        Arguments:
            search_term: text pattern to locate (e.g., "def test_function", "*SomeClass*")
        Output:
            locations where pattern was found with file paths and line numbers
        '''
        output = []

        # Walk through all directories and find Python files
        for root, _, files in os.walk("."):
            # Skip .git and docs directories
            if ".git" in root or "docs" in root:
                continue

            for file in files:
                if file.endswith('.py'):
                    file_path = os.path.join(root, file)
                    output.extend(self._search_in_file(file_path, search_term))

        output = "\n".join(output)
        output = Utils.limit_strings(output, n=100)
        if not output:
            raise ToolManager.Error(ToolManager.Error.ErrorType.SEARCH_TERM_NOT_FOUND.name,f"'{search_term}' not found in the codebase.")
        return output

    def get_function_ranges(self,file_path: str)->list[tuple[int, int, str]]:
        # Try to parse the file to map lines to their enclosing functions.
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                source_lines = f.read().splitlines()
        except Exception as e:
            raise ToolManager.Error(ToolManager.Error.ErrorType.FILE_NOT_FOUND.name,f"Error reading '{file_path}': {e}")
        try:
            tree = ast.parse("\n".join(source_lines), filename=file_path)
        except SyntaxError as e:
            raise ToolManager.Error(ToolManager.Error.ErrorType.SYNTAX_ERROR.name,f"Error parsing '{file_path}': {e}, {traceback.format_exc()}")
            tree = None  # Fallback if file cannot be parsed.

        func_ranges: list[tuple[int, int, str]] = []  # (start, end, name)
        if tree is not None:
            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    start = getattr(node, 'lineno', None)
                    end = getattr(node, 'end_lineno', None)
                    if start is not None and end is not None:
                        func_ranges.append((start, end, node.name))
        return func_ranges

    def _extract_function_matches(self,file_path: str, search_term: str, *, max_output_lines: int = 1000) -> str:
        '''
        Return the source code of any function definitions that contain `search_term`.
        If a match occurs outside of a function, only that line is returned. The final
        output is truncated with `limit_strings` to avoid excessive verbosity.
        '''
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                source_lines = f.read().splitlines()
        except Exception as e:
            logger.error(f"Error reading '{file_path}': {e}")
            raise ToolManager.Error(ToolManager.Error.ErrorType.FILE_NOT_FOUND.name,f"Error reading '{file_path}': {e}")

        # Identify all lines that contain the search term.
        match_lines = [idx + 1 for idx, line in enumerate(source_lines) if search_term in line]
        if not match_lines:
            raise ToolManager.Error(ToolManager.Error.ErrorType.SEARCH_TERM_NOT_FOUND.name,f"'{search_term}' not found in file '{file_path}'")

        func_ranges=self.get_function_ranges(file_path)

        def _containing_function(line_no: int):
            for start, end, name in func_ranges:
                if start <= line_no <= end:
                    return (start, end, name)
            return None

        functions_to_return: list[tuple[int, int, str]] = []
        standalone_lines: list[int] = []
        for ln in match_lines:
            info = _containing_function(ln)
            if info and info not in functions_to_return:
                functions_to_return.append(info)
            elif not info:
                standalone_lines.append(ln)

        chunks: list[str] = []
        for start, end, name in functions_to_return:
            func_src = "\n".join(source_lines[start - 1:end])
            chunks.append(f"(lines {start}-{end}):\n{func_src}")

        for ln in standalone_lines:
            chunks.append(f"{ln}:{source_lines[ln - 1]}")

        return Utils.limit_strings("\n\n".join(chunks), n=max_output_lines)

    @tool
    def search_in_specified_file_v2(self,file_path: str, search_term: str)->str:
        '''
        Locates text patterns within a specific file
        Arguments:
            file_path: target file for pattern matching. This file must be python file.
            search_term: text pattern to find (e.g., "def test_function", "*SomeClass*")
        Output:
            matching locations with line numbers, or error description
        '''
        if not file_path.endswith(".py"):
            raise ToolManager.Error(ToolManager.Error.ErrorType.INVALID_FILE_PATH.name,f"Error: file '{file_path}' is not a python file.")
        return self._extract_function_matches(file_path, search_term)

    @tool
    def search_recurive_in_all_files_in_directory(self, directory_path: str, search_term: str)->str:
        '''
        Locates text patterns recursively within all files in a specific directory
        Arguments:
            directory_path: target directory for pattern matching
            search_term: text pattern to find (e.g., "def test_function", "*SomeClass*")
        Output:
            matching locations with line numbers, or error description
        '''
        if not os.path.exists(directory_path) or not os.path.isdir(directory_path):
            raise ToolManager.Error(ToolManager.Error.ErrorType.FILE_NOT_FOUND.name,f"Error: directory '{directory_path}' does not exist.")
        output = []

        # Walk through all directories and find Python files
        for root, _, files in os.walk(directory_path):
            # Skip .git and docs directories
            if ".git" in root or "docs" in root:
                continue

            for file in files:
                if file.endswith('.py'):
                    file_path = os.path.join(root, file)
                    output.extend(self._search_in_file(file_path, search_term))

        output = "\n".join(output)
        output=Utils.limit_strings(output, n=100)
        if not output:
            raise ToolManager.Error(ToolManager.Error.ErrorType.SEARCH_TERM_NOT_FOUND.name,f"'{search_term}' not found in file '{directory_path}'")
        return output
    
    @tool
    def start_over(self,problem_with_old_approach:str,new_apprach_to_try:str):
        '''
        This will revert any changes made to the codebase and let's you start over. Only use this tool when you have concluded that current changes you made to the codebase are not relevant and you want to start again with new approach.
        Arguments:
            problem_with_old_approach: What you tried and what was the key issues you faced with this approach.
            new_apprach_to_try: What is the new approach you want to try and how it will fix the issues you faced earlier.
        '''    
        logger.info("============Start Over============")
        os.system("git reset --hard")
        logger.info(f"problem_with_old_approach: {problem_with_old_approach}")
        logger.info(f"new_apprach_to_try: {new_apprach_to_try}")
        logger.info("===========================")
        return "Done, codebase reverted to initial state. You can start over with new approach."
        
        
    def revert_any_moved_folders(self):
        for folder,new_folder in folders_moved:
            logger.info(f"reverting {new_folder} to {folder}")
            shutil.move(new_folder,folder)

    def get_final_git_patch(self)->str:
        '''
        Generates git diff patch containing all modifications in working directory
        Useful for capturing comprehensive change summary before finalization
        '''
        self.revert_any_moved_folders()
        output= subprocess.run(["bash", "-c", f"shopt -s globstar ; echo 'src/agent.py'> .gitignore; echo 'src/agent_runner.py'> .gitignore; git add **/*.py >/dev/null 2>&1 ; git diff --cached > .patch.txt ; cat .patch.txt"], timeout=30, capture_output=True)
        
        output=output.stdout.decode("utf-8")+'\n' + output.stderr.decode("utf-8")
        return output
    
    @tool
    def create_new_file(self,file_path:str, content:str)->str:
        '''
        Generates new file with specified content at target location. Do not use this tool to create test or files to reproduce the error unless user has specifically asked you to create test files as part of problem statement.
        Arguments:
            file_path: destination path for new file
            content: text content for file creation
        '''
        if "test" in file_path.lower() or "reproduce" in file_path.lower():
            raise ToolManager.Error(ToolManager.Error.ErrorType.INVALID_TOOL_CALL.name,f"Error: You cannot use this tool to create test or files to reproduce the error.")
        return self._save(file_path, content)

    @tool
    def run_code(self,content:str,file_path:str)->str:
        '''
        Runs any python code. You can use this tool directly to run any test code or bug reproduction code.
        Saves the code at the given file_path and then runs it. Do not use this tool to create test or files to reproduce the error unless user has specifically asked you to create test files as part of problem statement.

        Arguments:
            content: text code to write in file
            file_path: path of the file to save the code in. This file should always be in the current working directory.

        Output:
            Returns the stdout/stderr from the executed file.
            Returns error message if there are any third party dependencies.
        '''
        try:
            self._save(file_path, content)
        except Exception as e:
            raise ToolManager.Error(ToolManager.Error.ErrorType.SYNTAX_ERROR.name,f"Error saving code: {e}\n")
    
        # Parse the file's AST to collect import statements
        
        with open(file_path, "r") as f:
            tree = ast.parse(f.read(), filename=file_path)

        disallowed_modules = set()
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                # Use the module specified in 'from x import y' if available;
                # otherwise fall back to the imported name from plain 'import x'
                if isinstance(node, ast.ImportFrom) and node.module:
                    mod = node.module.split(".")[0]
                else:
                    mod = node.names[0].name.split(".")[0]

                # Skip if built-in module
                if mod in sys.builtin_module_names:
                    continue

               

                # Skip relative imports ("from . import foo") which have level > 0
                if isinstance(node, ast.ImportFrom) and node.level and node.level > 0:
                    continue

                # --- Additional check: allow local modules/packages in CWD ---
                cwd = os.getcwd()
                local_file = os.path.join(cwd, f"{mod}.py")
                local_pkg_init = os.path.join(cwd, mod, "__init__.py")
                local_pkg_dir = os.path.join(cwd, mod)
                # Also check inside a conventional 'lib' folder within cwd
                lib_dir = os.path.join(cwd, 'lib')
                lib_file = os.path.join(lib_dir, f"{mod}.py")
                lib_pkg_init = os.path.join(lib_dir, mod, "__init__.py")
                lib_pkg_dir = os.path.join(lib_dir, mod)

                if (
                    os.path.isfile(local_file)
                    or os.path.isfile(local_pkg_init)
                    or os.path.isdir(local_pkg_dir)
                    or os.path.isfile(lib_file)
                    or os.path.isfile(lib_pkg_init)
                    or os.path.isdir(lib_pkg_dir)
                ):
                    # Treat as local dependency, allow it
                    continue

                # Any other module is considered disallowed
                disallowed_modules.add(mod)
        
        result = subprocess.run(["python", file_path], capture_output=True, text=True, check=False, timeout=60)
        if result.returncode!=0:
            
            error_type=ToolManager.Error.ErrorType.RUNTIME_ERROR
            if "ImportError" in result.stderr:
                error_type=ToolManager.Error.ErrorType.IMPORT_ERROR
            if "ModuleNotFoundError" in result.stderr:
                error_type=ToolManager.Error.ErrorType.THIRD_PARTY_DEPENDENCIES
            raise ToolManager.Error(error_type,f"Error running code: {result.stderr}\n")
        
        if len(result.stdout) == 0:
            observation = f"Congratulations! It passed test successfully."
        else:
            observation = f"{result.stdout}\n"
       
        # Remove the file after it has been used
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except Exception as e:
                logger.warning(f"Could not remove file {file_path}: {e}")

        return observation
    
    @tool
    def apply_code_edit(self,file_path:str, search:str, replace:str)->str:
        '''
        Performs targeted text replacement within source files. If there are any syntax errors in the code, it rejects the edit with an error message. Please note use you can only use this tool after you have approval from user on your proposed solution.
        Arguments:
        file_path: target file for modification
        search: exact text pattern to locate and replace
        replace: new text content to substitute
            
        Output:
            operation status - success confirmation or detailed error with guidance
        '''
        if not self.is_solution_approved:
            raise ToolManager.Error(ToolManager.Error.ErrorType.INVALID_TOOL_CALL.name,f"Error: You cannot use this tool before you have approval from user on your proposed solution. Please call get_approval_for_solution tool first with list of proposed solutions.")
        if not os.path.exists(file_path):
            logger.error(f"file '{file_path}' does not exist.")
            raise ToolManager.Error(ToolManager.Error.ErrorType.FILE_NOT_FOUND.name,f"Error: file '{file_path}' does not exist.")
        
        original=self._get_file_content(file_path,limit=-1)

        match original.count(search):
            case 0:
                logger.error(f"search string not found in file {file_path}. You need to share the exact code you want to replace.")
                raise ToolManager.Error(ToolManager.Error.ErrorType.SEARCH_TERM_NOT_FOUND.name,f"Error: search string not found in file {file_path}. You need to share the exact code you want to replace.")
            case 1:
                
                new_content = original.replace(search, replace)
                try:
                        is_error,error=self.check_syntax_error(new_content)
                        if not is_error:
                            self.save_file(file_path, new_content)
                                
                            return "ok, code edit applied successfully"
                        else:
                            error.message="code edit failed. "+error.message
                            raise error
                except ToolManager.Error as e:
                    raise ToolManager.Error(ToolManager.Error.ErrorType.SYNTAX_ERROR.name,f"Error: syntax error in file {file_path}. {e.message}")
            case num_hits:
                logger.error(f"search string found {num_hits} times in file '{file_path}'.\nPlease reformulate your search and replace to apply only one change.")
                raise ToolManager.Error(ToolManager.Error.ErrorType.MULTIPLE_SEARCH_RESULTS_FOUND.name,f"Error: search string found {num_hits} times in file '{file_path}'.\nPlease reformulate your search and replace to apply only one change.")

    @tool
    def filter_test_func_names(self, reason_for_filtering: str, filtered_test_func_names: List[str]):
        '''
        Filter the list of test functions to keep the test functions that is specifically designed to test the scenario mentioned in the problem statement.
        Arguments:
            reason_for_filtering: The reason for filtering the list of test function names.
            filtered_test_func_names: The filtered list of test function names with file path (e.g. ["test_file_path.py - test_func_name", "test_file_path.py - test_func_name"])
        '''
        return "ok, test functions filtered successfully"

    @tool
    def sort_test_func_names(self, reason_for_sorting: str, sorted_test_func_names: List[str]):
        '''
        Sorts the list of test function names by their relevance to the issue mentioned in the problem statement in descending order.
        Arguments:
            reason_for_sorting: The reason for sorting the list of test function names.
            sorted_test_func_names: The sorted list of test function names with file path (e.g. ["test_file_path.py - test_func_name", "test_file_path.py - test_func_name"])
        '''
        return "ok, test function names sorted successfully"

    @tool
    def test_patch_find_finish(self, test_func_names: List[str]):
        '''
        Signals completion of the test patch find workflow execution
        Arguments:
            test_func_names: The list of test function names with file path (e.g. ["test_file_path.py - test_func_name", "test_file_path.py - test_func_name"])
        '''
        return "finish"

    @tool
    def llm_complete(self, prompt: str, system: str = "You are a helpful assistant.", temperature: float = 0.0, max_tokens: int = 1200) -> str:
        '''
        Call the underlying LLM to reason or draft content. Does NOT browse the web.
        Arguments:
            prompt: user-facing instruction or content to transform.
            system: optional system primer to steer style/role.
            temperature: decoding temperature (0.0–1.0 typical).
            max_tokens: response length hint (best-effort).
        Output:
            Raw model text response.
        '''
        messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": prompt}
        ]
        return Network.make_request(messages)

    @tool
    def structured_llm(self, instruction: str, schema_hint: str = "") -> str:
        '''
        Ask LLM to return strictly valid JSON and parse it.
        Arguments:
            instruction: what structure you want (e.g., {"files":[], "edits":[]}).
            schema_hint: optional schema/example JSON to nudge formatting.
        Output:
            A valid JSON string if parsing succeeds; otherwise an error string.
        '''
        sys_msg = "Reply ONLY with strictly valid JSON. Do not include code fences or commentary."
        user_msg = instruction if not schema_hint else f"{instruction}\n\nJSON schema/example:\n{schema_hint}"
        messages = [{"role":"system","content":sys_msg},{"role":"user","content":user_msg}]
        raw = Network.make_request(messages)
        try:
            parsed = Utils.load_json(raw.replace("```json","").replace("```","").strip())
            return json.dumps(parsed, ensure_ascii=False)
        except Exception as e:
            return f"Error: invalid JSON from model: {e}\nRaw:\n{raw}"

    @tool
    def run_repo_tests(self, command: str = "python -m pytest -q", timeout_secs: int = 420) -> str:
        '''
        Run repository tests to validate edits.
        Arguments:
            command: shell command for tests (default: python -m pytest -q).
            timeout_secs: cap execution time.
        Output:
            Combined stdout/stderr (last 200 lines if long).
        '''
        try:
            proc = subprocess.run(
                ["bash", "-c", command],
                capture_output=True,
                text=True,
                timeout=timeout_secs
            )
            out = (proc.stdout or "") + (proc.stderr or "")
            lines = out.splitlines()
            if len(lines) > 200:
                out = "\n".join(lines[-200:]) + f"\n...[truncated {len(lines)-200} lines]"
            if proc.returncode != 0:
                return f"TESTS FAILED (exit {proc.returncode})\n{out}"
            return f"TESTS PASSED\n{out}"
        except subprocess.TimeoutExpired:
            return "Error: tests timed out."

    @tool
    def compile_repo(self) -> str:
        '''
        Byte-compile all Python files to catch syntax errors quickly.
        Arguments:
            None
        Output:
            "OK" on success or error details on failure.
        '''
        try:
            ls = subprocess.run(["bash","-c","git ls-files '*.py'; ls -1 **/*.py 2>/dev/null | cat"], capture_output=True, text=True)
            files = sorted(set([p for p in ls.stdout.splitlines() if p.strip().endswith(".py")]))
            if not files:
                return "No Python files found."
            cmd = ["python","-m","py_compile"] + files
            proc = subprocess.run(cmd, capture_output=True, text=True)
            if proc.returncode != 0:
                return f"COMPILE ERRORS\n{proc.stderr or proc.stdout}"
            return "OK"
        except Exception as e:
            return f"Error during compile: {e}"

    @tool
    def grep_replace_once(self, file_path: str, pattern: str, replacement: str, flags: str = "") -> str:
        '''
        Regex-based single replacement with safety checks.
        Arguments:
            file_path: file to edit (py or text).
            pattern: regex to find (must match exactly one region).
            replacement: replacement text (supports backrefs).
            flags: optional re flags: "I" (IGNORECASE), "M" (MULTILINE), "S" (DOTALL).
        Output:
            "ok" or a descriptive error message.
        '''
        if not os.path.exists(file_path):
            return f"Error: file '{file_path}' does not exist."
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                original = f.read()
            fset = 0
            if "I" in flags: fset |= re.IGNORECASE
            if "M" in flags: fset |= re.MULTILINE
            if "S" in flags: fset |= re.DOTALL
            matches = list(re.finditer(pattern, original, fset))
            if len(matches) == 0:
                return "Error: pattern not found."
            if len(matches) > 1:
                return f"Error: pattern matched {len(matches)} times; refusing to change multiple locations."
            new_content = re.sub(pattern, replacement, original, count=1, flags=fset)
            if file_path.endswith(".py"):
                try:
                    ast.parse(new_content, filename=file_path)
                except SyntaxError as e:
                    return f"Error: replacement causes syntax error: {e}"
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(new_content)
            return "ok"
        except Exception as e:
            return f"Error editing file: {e}"

    @tool
    def list_python_files(self) -> str:
        '''
        List Python files in the repo (tracked and untracked).
        Arguments:
            None
        Output:
            Newline-separated list of paths.
        '''
        res = subprocess.run(["bash","-c","git ls-files '*.py'; ls -1 **/*.py 2>/dev/null | cat"], capture_output=True, text=True)
        paths = sorted(set([p for p in res.stdout.splitlines() if p.strip().endswith(".py")]))
        return "\n".join(paths) if paths else "No Python files found."

    @tool
    def finish(self,investigation_summary: str):
        '''
        Signals completion of the current workflow execution
        Arguments:
            investigation_summary: Please provide a detailed summary of the findings from your investigation and detailed solution to the problem.Use the following format:
                Problem: <problem_statement>
                Investigation: <investigation_summary>
                Solution: <your solution>
        '''
        #patch=get_final_git_patch()
        #qa_response=QA.fetch_qa_response(investigation_summary,patch)
        qa_response={"is_patch_correct":"yes"}
        if qa_response.get("is_patch_correct","no").lower()=="yes":
            return "finish"
        else: 
            raise ToolManager.Error(ToolManager.Error.ErrorType.BUG_REPORT_REQUIRED.name,qa_response.get("analysis",""))
    
    @classmethod
    def get_tool_args_for_tool(cls,tool_name:str,required_only:bool=False)->list[str]:
        if tool_name not in cls.TOOL_LIST:
            raise ToolManager.Error(ToolManager.Error.ErrorType.INVALID_TOOL_NAME.name,f"Error: tool '{tool_name}' not found")
        if not required_only: 
            return list(cls.TOOL_LIST[tool_name]['input_schema']['properties'].keys())
        else:
            return cls.TOOL_LIST[tool_name]['input_schema']['required']
    
    # Add new parallel execution tools
    @tool
    def parallel_codebase_analysis(self, file_paths: List[str], search_terms: List[str]) -> str:
        '''
        Perform comprehensive codebase analysis using parallel execution
        Arguments:
            file_paths: List of files to analyze
            search_terms: List of terms to search for
        Output:
            Comprehensive analysis results from parallel execution
        '''
        try:
            self.performance_monitor.start_timer("parallel_analysis")
            
            # Execute multiple analyses in parallel
            analysis_results = self.parallel_executor.execute_parallel_analysis(
                file_paths[0] if file_paths else ".",
                []  # test_func_names will be determined later
            )
            
            # Search for multiple terms in parallel
            search_results = self.file_searcher.search_multiple_files_parallel(search_terms)
            
            # Get multiple file contents in parallel
            file_contents = self.file_processor.get_multiple_file_contents_parallel(file_paths[:5])
            
            # Combine all results
            combined_results = {
                'analysis': analysis_results,
                'search': search_results,
                'file_contents': file_contents
            }
            
            self.performance_monitor.end_timer("parallel_analysis")
            
            return json.dumps(combined_results, indent=2)
            
        except Exception as e:
            raise ToolManager.Error(
                ToolManager.Error.ErrorType.RUNTIME_ERROR.name,
                f"Parallel analysis failed: {e}"
            )
    
    @tool
    def parallel_test_discovery(self, problem_statement: str) -> str:
        '''
        Discover test functions using parallel search strategies
        Arguments:
            problem_statement: The problem to find tests for
        Output:
            List of relevant test functions found through parallel search
        '''
        try:
            self.performance_monitor.start_timer("parallel_test_discovery")
            
            # Extract key terms from problem statement
            key_terms = self._extract_key_terms(problem_statement)
            
            # Search for multiple patterns in parallel
            search_patterns = [
                f"def test_{term}" for term in key_terms
            ] + [
                f"class Test{term.capitalize()}" for term in key_terms
            ] + [
                f"assert {term}" for term in key_terms
            ]
            
            search_results = self.file_searcher.search_multiple_files_parallel(search_patterns)
            
            # Analyze results to find most relevant test functions
            relevant_tests = self._identify_relevant_tests(search_results, problem_statement)
            
            self.performance_monitor.end_timer("parallel_test_discovery")
            
            return json.dumps({
                'search_results': search_results,
                'relevant_tests': relevant_tests
            }, indent=2)
            
        except Exception as e:
            raise ToolManager.Error(
                ToolManager.Error.ErrorType.RUNTIME_ERROR.name,
                f"Parallel test discovery failed: {e}"
            )
    
    @tool
    def parallel_file_operations(self, file_paths: List[str], operations: List[str]) -> str:
        '''
        Perform multiple file operations in parallel
        Arguments:
            file_paths: List of files to operate on
            operations: List of operations to perform (read, analyze, search)
        Output:
            Results of parallel file operations
        '''
        try:
            self.performance_monitor.start_timer("parallel_file_operations")
            
            results = {}
            
            # Get file contents in parallel
            if 'read' in operations:
                file_contents = self.file_processor.get_multiple_file_contents_parallel(file_paths)
                results['file_contents'] = file_contents
            
            # Analyze files in parallel
            if 'analyze' in operations:
                analysis_tasks = {}
                for file_path in file_paths:
                    analysis_tasks[f'analyze_{file_path}'] = lambda fp=file_path: self._analyze_single_file(fp)
                
                analysis_results = self.dependency_executor._execute_parallel(analysis_tasks)
                results['analysis'] = analysis_results
            
            # Search in files in parallel
            if 'search' in operations:
                search_terms = ['def ', 'class ', 'import ', 'from ']
                search_results = self.file_searcher.search_multiple_files_parallel(search_terms)
                results['search'] = search_results
            
            self.performance_monitor.end_timer("parallel_file_operations")
            
            return json.dumps(results, indent=2)
            
        except Exception as e:
            raise ToolManager.Error(
                ToolManager.Error.ErrorType.RUNTIME_ERROR.name,
                f"Parallel file operations failed: {e}"
            )
    
    @tool
    def get_performance_metrics(self) -> str:
        '''
        Get performance metrics from parallel operations
        Arguments:
            None
        Output:
            Performance summary and metrics
        '''
        try:
            performance_summary = self.performance_monitor.get_performance_summary()
            return performance_summary
        except Exception as e:
            return f"Error getting performance metrics: {e}"
    
    def _extract_key_terms(self, problem_statement: str) -> List[str]:
        """Extract key terms from problem statement for search"""
        # Simple keyword extraction - could be enhanced with NLP
        words = problem_statement.lower().split()
        # Filter out common words and keep meaningful terms
        stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by'}
        key_terms = [word for word in words if word not in stop_words and len(word) > 3]
        return list(set(key_terms))[:5]  # Limit to top 5 terms
    
    def _identify_relevant_tests(self, search_results: Dict[str, str], problem_statement: str) -> List[str]:
        """Identify the most relevant test functions from search results"""
        relevant_tests = []
        
        for pattern, result in search_results.items():
            if "Error" not in result:
                # Parse the result to extract file paths and function names
                lines = result.split('\n')
                for line in lines:
                    if ':' in line and 'def test_' in line:
                        file_path = line.split(':')[0]
                        func_name = line.split('def ')[1].split('(')[0]
                        relevant_tests.append(f"{file_path} - {func_name}")
        
        return relevant_tests[:10]  # Limit to top 10 most relevant
    
    def _analyze_single_file(self, file_path: str) -> Dict[str, Any]:
        """Analyze a single file with multiple tools"""
        try:
            return {
                'content': self.get_file_content(file_path, limit=1000),
                'smells': self.detect_code_smells(file_path),
                'quality': self.get_code_quality_metrics(file_path)
            }
        except Exception as e:
            return {'error': str(e)}

TEST_PATCH_FIND_SYSTEM_PROMPT_TEMPLATE = textwrap.dedent("""
# 🧠 Test Function Finder
You are a code analysis expert tasked with identifying test functions that directly validate the issue described in the problem statement. Follow this structured workflow:

**🔍 Step-by-Step Process**
1. **Problem Analysis** 
   - Parse the problem statement Carefully
   - Read "Hints" carefully if it exists. It will helpful for solving problems.   
   - Identify affected functions/classes
   - Note expected input/output behaviors

2. **Test Discovery**
   - Use `search_in_all_files_content_v2` with multiple search strategies
   - Use `analyze_test_coverage` to verify test relevance
   - Use `analyze_dependencies` to understand test relationships

3. **Filtering & Ranking** 
   - Remove irrelevant test functions
   - Rank by test specificity, coverage, and isolation

4. **Validation**
   - Confirm test functions fail with the described issue

**🛠️ Available Tools**
- `search_in_all_files_content_v2`: Find test patterns across the repo
- `analyze_test_coverage`: Verify test coverage of proposed functions
- `analyze_dependencies`: Understand test function relationships
- `get_file_content`: Retrieve test function source code
- `test_patch_find_finish`: Finalize test function list
- `parallel_codebase_analysis`: Perform comprehensive analysis using parallel execution
- `parallel_test_discovery`: Discover test functions using parallel search strategies
- `parallel_file_operations`: Perform multiple file operations in parallel
- `get_performance_metrics`: Get performance metrics from parallel operations

**⚠️ Critical Rules**
- Only return test functions that explicitly validate the problem
- Use `analyze_git_history` to understand historical context of test failures
- Prioritize tests with clear assertions and minimal setup
- If no relevant tests exist, return the most likely candidate with `analyze_test_coverage` validation
- Always use the exact tool names from the provided documentation (e.g., `search_in_specified_file_v2`, not `search_in_specified_file`)
- Never guess parameter names; refer to the tool's input schema
- If a tool is not available, explicitly state it and proceed to the next step

You have access to the following tools:-
{tools_docs}

{format_prompt}
""")



SYSTEM_PROMPT_TEMPLATE = textwrap.dedent("""
# 🛠️ Code Fixing Expert
You are a senior Python developer tasked with resolving the issue described in the problem statement while ensuring all provided test functions pass. Follow this structured workflow:
You will receive:
1. A **problem statement**.
2. The **specific test functions** your fix must pass.

Your task: Make the necessary code changes to resolve the issue and pass the provided tests.

---

## 🔹 Key Rules
- Only check **test files mentioned in the provided test functions** — ignore all other test files.
- Always reference both the **problem statement** and the provided tests when deciding what to modify.
- Never edit or create test files, new files, or directories.
- Code must remain **backward compatible** unless the problem statement says otherwise.
- Handle **edge cases** and ensure the fix does not break other functionality.
- Propose **at least two** accurate, meaningfully different solutions for the user to approve before implementing.
- Look at both:
  1. The expected output in the problem statement.
  2. The expected output in the most relevant test case.
- If a `run_code` tool error occurs due to missing dependencies, **do not** attempt to install them (no internet access).
- Never assume a patch works without running tests
- Always validate test functions cover the problem area
- If tests fail, analyze the failure and propose fixes carefully
---

## 🔹 Workflow
1. Identify relevant files based on the given test functions and problem statement.
2. Locate the code responsible for the issue.
3. Modify the source code to fix the problem.
4. Ensure edge cases are handled.
5. Validate changes across the codebase for completeness and safety.
6. Confirm no unrelated changes were made.
7. Get approval from the user before applying your chosen solution.

**🔧 Implementation** 
1. Use `apply_code_edit` for precise changes
2. Use `grep_replace_once` for simple regex fixes
3. Use `get_approval_for_solution` before implementing
4. Use `start_over` if current approach is invalid

**✅ Validation** 
1. Run `validate_solution` to confirm test function results
2. Use `run_repo_tests` to verify fixes
3. Use `detect_code_smells` to verify no new smells introduced
---

You have access to the following tools:
{tools_docs}

{format_prompt}
""")

FORMAT_PROMPT=textwrap.dedent("""
**📝 Response Format Requirements**

1. **Strict Triplet Format**:
   - `next_thought`: Detailed reasoning (include:
     - Problem understanding
     - Code analysis
     - Solution justification
     - Validation plan)
   - `next_tool_name`: Must be an exact tool name from the tool list
   - `next_tool_args`: Valid JSON with:
     - Proper escaping
     - No trailing commas
     - Tool-specific parameters

2. **Error Handling Format**:
   - For errors: 
     next_thought: "Error: [detailed explanation]"
     next_tool_name: ""
     next_tool_args: {}

3. **Example Valid Format**:
   next_thought: "I'll fix the JSON parsing issue by adding proper error handling and validation"
   next_tool_name: "apply_code_edit"
   next_tool_args: {
     "file_path": "network.py",
     "search": "return json.loads(response)",
     "replace": "try:\n    return json.loads(response)\nexcept JSONDecodeError:\n    logger.error(f'Invalid JSON: {response}')\n    raise"
   }

4. **Invalid Format Examples** (Avoid These):
   - Incorrect next_tool_name such as "search_in_all_files_content" instead correct tool name - "search_in_all_files_content_v2"
   - Missing any of the three required fields
   - JSON syntax errors in next_tool_args
   - Extra text outside the triplet format
   - Using incorrect tool names
   - Not quoting special characters properly
""")

PATCH_FIND_INSTANCE_PROMPT_TEMPLATE = textwrap.dedent("""
# Now let's start. Here is the problem statement:
{problem_statement}
""")

INSTANCE_PROMPT_TEMPLATE = textwrap.dedent("""
# Now let's start. Here are the test functions you need to pass:
{test_func_codes}

# Here is the problem statement:
{problem_statement}
""")

STOP_INSTRUCTION=textwrap.dedent("""
# 🎨 
DO NOT generate `observation:` in your response. It will be provided by user for you.
Generate only SINGLE triplet of `next_thought`, `next_tool_name`, `next_tool_args` in your response.
""")

DEFAULT_PROXY_URL = os.getenv("AI_PROXY_URL", "http://sandbox-proxy")
DEFAULT_TIMEOUT = int(os.getenv("AGENT_TIMEOUT", "1200"))
AGENT_MODELS=["zai-org/GLM-4.5-FP8", "deepseek-ai/DeepSeek-V3-0324"]

MAX_STEPS = 150
MAX_STEPS_TEST_PATCH_FIND = 100
DEBUG_MODE=True



def process_task(input_dict: Dict[str, Any], repod_dir: str = 'repo'):
    """Main entry point for task processing and code modification.

    Parameters
    ----------
    input_dict : dict
        Configuration dictionary containing the task specification.
        Required key: 'problem_statement' with task details.
        Optional keys: 'run_id', 'instance_id' for tracking purposes.
    """
    # setting environment to include current working directory and lib directory
    
    problem_text = input_dict.get("problem_statement")
    if not problem_text:
        raise ValueError("input_dict must contain 'problem_statement'.")
    timeout = int(os.getenv("AGENT_TIMEOUT", str(DEFAULT_TIMEOUT)))
    
    logs = []
    _logs_patch_find_workflow = []
    _logs_patch_workflow = []
    patch_text = ""  # Initialize to avoid UnboundLocalError
    test_func_names = []
    
    if os.path.exists(repod_dir):
        os.chdir(repod_dir)

    
    set_env_for_agent()
    logger.info(f"Current working directory: {os.getcwd()} and environ:{os.environ}")
    try:
        if not DEBUG_MODE:
            os.system("git reset --hard")
        os.system("git config --global --add safe.directory /sandbox/repo")
        os.system("git config --global --add safe.directory /sandbox")
        logger.info(f"current files:{os.listdir()}")
        logger.info(f"packages installed:{subprocess.check_output(['pip','list']).decode('utf-8')}")
        logger.info(f"About to execute workflow...")

        try:
            test_func_names, _logs_patch_find_workflow = execute_test_patch_find_workflow(
                problem_text,
                timeout=timeout, 
                run_id_1=input_dict.get("run_id", ""), 
                instance_id=input_dict.get("instance_id", "")
            )
        except Exception as e:
            logger.error(f"Error in test_patch_find_workflow: {e}")
            test_func_names = []
            _logs_patch_find_workflow = []

        logs += _logs_patch_find_workflow
        
        tool_manager = ToolManager()
        
        test_func_codes = []
        for test_func_name in test_func_names:
            file_path, function_name = test_func_name.split(" - ")
            function_name = function_name.split(".")[-1]
            test_func_codes.append(f"```{file_path}\n\n{tool_manager.get_function_body(file_path, function_name)}\n```")

        logger.info(f"test_func_codes: {test_func_codes}")
        logs.append(f"test_func_codes: {test_func_codes}\n\n")

        patch_text, _logs_patch_workflow = execute_workflow(
                problem_text,
                timeout=timeout,
                run_id_1=input_dict.get("run_id", ""),
                instance_id=input_dict.get("instance_id", ""),
                test_func_codes=test_func_codes
            )
        logger.info(f"workflow execution completed, patch length: {len(patch_text)}")
        logs += _logs_patch_workflow

        os.system("git reset --hard")

    except Exception as e:
        import traceback  # Ensure traceback is accessible
        error_info = f"Error: {e}, {traceback.format_exc()}"
        logger.error(f"[CRITICAL] Exception in task processing: {error_info}")
        logs.append(error_info)

    print(f"[CRITICAL] task processor returning patch length: {len(patch_text)}")
    return {"patch": patch_text, "test_func_names": test_func_names, "logs": logs}

def agent_main(input_dict: Dict[str, Any], repo_dir: str = "repo"):
    """Legacy interface wrapper for backwards compatibility."""
    repo_dir = os.path.abspath(repo_dir)
    return process_task(input_dict, repo_dir)

def set_env_for_agent():
    if os.getcwd() not in os.environ.get("PYTHONPATH",""):
        os.environ["PYTHONPATH"]=os.environ.get("PYTHONPATH","")+":"+os.getcwd()
    if Path(os.getcwd()+"/lib").exists() and os.getcwd()+"/lib" not in os.environ.get("PYTHONPATH",""):
        os.environ["PYTHONPATH"]=os.environ["PYTHONPATH"]+":"+os.getcwd()+"/lib"

def execute_test_patch_find_workflow(problem_statement: str, *, timeout: int, run_id_1: str, instance_id: str = "") -> tuple[List[str], List[str]]:
    global run_id
    run_id=run_id_1
    cot=COT(latest_observations_to_keep=500)
    tool_manager=ToolManager(
        available_tools=[
            "search_in_all_files_content_v2",
            "analyze_test_coverage",
            "analyze_dependencies",
            "get_file_content",
            "search_in_specified_file_v2",
            "search_recurive_in_all_files_in_directory",
            "test_patch_find_finish",
            "sort_test_func_names",
            "filter_test_func_names",
            "parallel_codebase_analysis",
            "parallel_test_discovery",
            "parallel_file_operations",
            "get_performance_metrics",
        ]
    )
    logger.info(f"[TEST_PATCH_FIND] Starting test patch find agent execution...")
    system_prompt = TEST_PATCH_FIND_SYSTEM_PROMPT_TEMPLATE.format(tools_docs=ToolManager.get_tool_docs(),format_prompt=FORMAT_PROMPT)
    instance_prompt = PATCH_FIND_INSTANCE_PROMPT_TEMPLATE.format(problem_statement=problem_statement)

    #QA.SYSTEM_PROMPT=QA.SYSTEM_PROMPT.format(problem_statement=problem_statement)
    
    start_time = time.time()
    logs: List[str] = []

    for step in range(MAX_STEPS_TEST_PATCH_FIND):
        logger.info(f"[TEST_PATCH_FIND] Execution step {step + 1}/{MAX_STEPS_TEST_PATCH_FIND}")
        
        if time.time() - start_time > timeout:
            cot.add_action(COT.Action(next_thought="global timeout reached",next_tool_name="",next_tool_args={},observation="",is_error=True,inference_error_counter={},request_data=[]))
            break
        
        logs.append(f"Execution step {step + 1}/{MAX_STEPS_TEST_PATCH_FIND}\n\n")

        messages: List[Dict[str, Any]] = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": instance_prompt},
            ]
        
        messages.extend(cot.to_str())

        messages.append({"role": "system", "content": STOP_INSTRUCTION})
    
        try:
            next_thought, next_tool_name, next_tool_args,raw_text,total_attempts,error_counter,messages = Network.inference(messages, run_id=run_id)
            logs.append(f"next_thought: {next_thought}\n\nnext_tool_name: {next_tool_name}\n\nnext_tool_args: {next_tool_args}\n\n")
        except Exception as e:
            import traceback  # Ensure traceback is accessible
            error_msg=f"\n\nERROR: {repr(e)} {traceback.format_exc()}"
            logs.append(f"Inference error: {error_msg}\n\n")
            logger.error(f"[TEST_PATCH_FIND] Inference error: {error_msg}")
            cot.add_action(COT.Action(next_thought=error_msg,next_tool_name="",next_tool_args={},observation="",is_error=True,raw_response=raw_text,total_attempts=total_attempts, inference_error_counter=error_counter,request_data=messages))
            break
        
        logger.info(f"[TEST_PATCH_FIND] About to execute operation: {next_tool_name}")
       
        try:
            logger.info(f"[TEST_PATCH_FIND] next_thought: {next_thought}\nnext_tool_name: {next_tool_name}\nnext_tool_args: {next_tool_args}\n")
            if '"' in next_tool_name or "'" in next_tool_name:
                next_tool_name=next_tool_name.replace('"','')
                next_tool_name=next_tool_name.replace("'","")
                
            next_observation = tool_manager.get_tool(next_tool_name)(**next_tool_args) if next_tool_args else tool_manager.get_tool(next_tool_name)()
            logs.append(f"next_observation: {next_observation}\n\n")
            logger.info(f"[TEST_PATCH_FIND] next_observation: {next_observation}")
            cot.add_action(COT.Action(next_thought=next_thought,next_tool_name=next_tool_name,next_tool_args=next_tool_args,observation=next_observation,is_error=False,raw_response=raw_text,total_attempts=total_attempts,inference_error_counter=error_counter,request_data=messages))
        except ToolManager.Error as e:
            import traceback  # Ensure traceback is accessible
            error_msg=f"observation: {e.message}"
            logs.append(f"Tool error: {error_msg}\n\n")
            logger.error(f"[TEST_PATCH_FIND] Tool error: {error_msg}")
            cot.add_action(COT.Action(next_thought=next_thought,next_tool_name=next_tool_name,next_tool_args=next_tool_args,observation=error_msg,is_error=True,raw_response=raw_text,total_attempts=total_attempts,inference_error_counter=error_counter,request_data=messages))
            continue
        except Exception as e:
            import traceback  # Ensure traceback is accessible
            error_traceback=traceback.format_exc()
            if isinstance(e,TypeError):
                error_msg=f"observation: {str(e)}"
            else:
                error_msg=f"observation: {repr(e)} {error_traceback}"
            logs.append(f"Tool error: {error_msg}\n\n")
            logger.error(f"[TEST_PATCH_FIND] Tool error: {error_msg}")
            cot.add_action(COT.Action(next_thought=next_thought,next_tool_name=next_tool_name,next_tool_args=next_tool_args,observation=error_msg,is_error=True,raw_response=raw_text,total_attempts=total_attempts,inference_error_counter=error_counter,request_data=messages))
            continue
        
        if next_tool_name == "test_patch_find_finish":
            test_func_names = next_tool_args["test_func_names"]
            logger.info(f'[TEST_PATCH_FIND] [CRITICAL] Workflow called test_patch_find_finish operation with test_func_names: {test_func_names}')
            logs.append(f"Workflow called test_patch_find_finish operation with test_func_names: {test_func_names}\n\n")
            return test_func_names, logs
        print(f"[TEST_PATCH_FIND] [CRITICAL] Completed step {step + 1}, continuing to next step")
    else:
        # This happens if we exit the loop without breaking (reached MAX_STEPS)
        cot.add_action(COT.Action(next_thought="global timeout reached",next_tool_name="",next_tool_args={},observation="",is_error=True))
        logger.info(f"[TEST_PATCH_FIND] [CRITICAL] Workflow completed after reaching MAX_STEPS ({MAX_STEPS_TEST_PATCH_FIND})")
    
def execute_workflow(problem_statement: str, *, timeout: int, run_id_1: str, instance_id: str = "", test_func_codes: List[tuple[str, str, str]] = None) -> tuple[str, List[str], List[str]]:
    global run_id
    run_id=run_id_1
    cot=COT(latest_observations_to_keep=1000)
    tool_manager=ToolManager(
        available_tools=[
            "search_in_all_files_content_v2",
            "analyze_test_coverage",
            "analyze_dependencies",
            "detect_code_smells",
            "analyze_git_history",
            "get_code_quality_metrics",
            "validate_solution",
            "propose_solutions",
            "compare_solutions",
            "apply_code_edit",
            "grep_replace_once",
            "get_approval_for_solution",
            "run_repo_tests",  # Added for validation
            "start_over",
            "finish",
            "parallel_codebase_analysis",
            "parallel_test_discovery",
            "parallel_file_operations",
            "get_performance_metrics",
        ]
    )
    logger.info(f"Startingmain agent execution...")
    system_prompt = SYSTEM_PROMPT_TEMPLATE.format(tools_docs=ToolManager.get_tool_docs(),format_prompt=FORMAT_PROMPT)
    instance_prompt = INSTANCE_PROMPT_TEMPLATE.format(problem_statement=problem_statement, test_func_codes="\n\n".join(test_func_codes))

    logger.info(f"instance_prompt: {instance_prompt}")

    #QA.SYSTEM_PROMPT=QA.SYSTEM_PROMPT.format(problem_statement=problem_statement)
    
    start_time = time.time()
    logs: List[str] = []
    logs.append(f"cwd: {os.getcwd()}")
    logger.info(f"Starting workflow execution with {MAX_STEPS} max steps: timeout: {timeout} seconds : run_id: {run_id}")

    for step in range(MAX_STEPS):
        logger.info(f"Execution step {step + 1}/{MAX_STEPS}")
        
        if time.time() - start_time > timeout:
            cot.add_action(COT.Action(next_thought="global timeout reached",next_tool_name="",next_tool_args={},observation="",is_error=True,inference_error_counter={},request_data=[]))
            break

        messages: List[Dict[str, Any]] = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": instance_prompt},
            ]
        
        messages.extend(cot.to_str())

        messages.append({"role": "system", "content": STOP_INSTRUCTION})
    
        try:
            next_thought, next_tool_name, next_tool_args,raw_text,total_attempts,error_counter,messages = Network.inference(messages, run_id=run_id)
            logs.append(f"next_thought: {next_thought}\n\nnext_tool_name: {next_tool_name}\n\nnext_tool_args: {next_tool_args}\n\n")
        except Exception as e:
            import traceback  # Ensure traceback is accessible
            error_msg=f"\n\nERROR: {repr(e)} {traceback.format_exc()}"
            logs.append(f"Inference error: {error_msg}\n\n")
            logger.error(f"Inference error: {error_msg}")
            cot.add_action(COT.Action(next_thought=next_thought,next_tool_name=next_tool_name,next_tool_args=next_tool_args,observation=error_msg,is_error=True,raw_response=raw_text,total_attempts=total_attempts,inference_error_counter=error_counter,request_data=messages))
            break
        
        logger.info(f"About to execute operation: {next_tool_name}")
       
        try:
            logger.info(f"next_thought: {next_thought}\nnext_tool_name: {next_tool_name}\nnext_tool_args: {next_tool_args}\n")
            if '"' in next_tool_name or "'" in next_tool_name:
                next_tool_name=next_tool_name.replace('"','')
                next_tool_name=next_tool_name.replace("'","")
                
            next_observation = tool_manager.get_tool(next_tool_name)(**next_tool_args) if next_tool_args else tool_manager.get_tool(next_tool_name)()
            logs.append(f"next_observation: {next_observation}\n\n")
            logger.info(f"next_observation: {next_observation}")
            cot.add_action(COT.Action(next_thought=next_thought,next_tool_name=next_tool_name,next_tool_args=next_tool_args,observation=next_observation,is_error=False,raw_response=raw_text,total_attempts=total_attempts,inference_error_counter=error_counter,request_data=messages))
        except ToolManager.Error as e:
            import traceback  # Ensure traceback is accessible
            error_msg=f"observation: {e.message}"
            logs.append(f"Tool error: {error_msg}\n\n")
            logger.error(f"Tool error: {error_msg}")
            cot.add_action(COT.Action(next_thought=next_thought,next_tool_name=next_tool_name,next_tool_args=next_tool_args,observation=error_msg,is_error=True,raw_response=raw_text,total_attempts=total_attempts,inference_error_counter=error_counter,request_data=messages))
            continue
        except Exception as e:
            import traceback  # Ensure traceback is accessible
            error_traceback=traceback.format_exc()
            if isinstance(e,TypeError):
                error_msg=f"observation: {str(e)}"
            else:
                error_msg=f"observation: {repr(e)} {error_traceback}"
            logs.append(f"Tool error: {error_msg}\n\n")
            logger.error(f"Tool error: {error_msg}")
            cot.add_action(COT.Action(next_thought=next_thought,next_tool_name=next_tool_name,next_tool_args=next_tool_args,observation=error_msg,is_error=True,raw_response=raw_text,total_attempts=total_attempts,inference_error_counter=error_counter,request_data=messages))
            continue
        
        if next_tool_name == "finish":
            logs.append(f"Workflow called finish operation\n\n")
            logger.info('[CRITICAL] Workflow called finish operation')
            break
        logs.append(f"Completed step {step + 1}, continuing to next step\n\n")
        print(f"[CRITICAL] Completed step {step + 1}, continuing to next step")
    else:
        # This happens if we exit the loop without breaking (reached MAX_STEPS)
        cot.add_action(COT.Action(next_thought="global timeout reached",next_tool_name="",next_tool_args={},observation="",is_error=True))
        logger.info(f"[CRITICAL] Workflow completed after reaching MAX_STEPS ({MAX_STEPS})")
    
    logger.info(f"[CRITICAL] Workflow execution completed after {step + 1} steps")
    logger.info(f"[CRITICAL] About to generate final patch...")
    patch = tool_manager.get_final_git_patch()
    logger.info(f"Final Patch Generated..: Length: {len(patch)}")
    logger.info(f"Final Patch: {patch}")
    logs.append(f"Final Patch: {patch}\n\n")
    

    return patch, logs