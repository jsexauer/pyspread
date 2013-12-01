#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Jason Sexauer, Martin Manns
# Distributed under the terms of the GNU General Public License

# --------------------------------------------------------------------
# pyspread is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# pyspread is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with pyspread.  If not, see <http://www.gnu.org/licenses/>.
# --------------------------------------------------------------------

"""

Eval Manager
=====

Coordinates the eval/exec statements used to run user code from the spreadsheet
and macros by queing them and allowing them to go into multiple processes.

Good references:
 http://wiki.wxpython.org/MultiProcessing
 http://broadcast.oreilly.com/2009/04/pymotw-multiprocessing-part-2.html
"""

import sys
import multiprocessing
import wx
from multiprocessing.queues import Queue, Empty as QueueEmpty
from traceback import print_exception
from StringIO import StringIO

import time


class MsgTypes(object):
    INFO, RESULT, STATUS, USER_ERROR = range(4)
    STARTED, FINISHED = [10,11]
class PrMsg(object):
    def __init__(self, msg, type=MsgTypes.INFO):
        self.msg = msg
        self.type = type
        self._timestamp = time.strftime("%c")

        # Defined by the Message Queue at worker signing
        self.worker = None
        self.task = None

    def log(self):
        # Append me to the log
        f = open(r"C:\log.txt", 'a')
        f.write(str(self)+'\n')
        f.close()


    @property
    def type_name(self):
        a = [k for k,v in MsgTypes.__dict__.iteritems() if v == self.type]
        if len(a) == 1:
            return a[0]
        else:
            return 'UNKNOWN'

    def __str__(self):
        s = ("[{0} {1}]".format(self._timestamp,
                                   self.type_name).ljust(35)
                + self.worker.ljust(10) + " doing " + str(self.task))
        if self.msg != '':
            s += "\n\t>> " + str(self.msg)
        return s



class Worker(multiprocessing.Process):
    def __init__(self, task_queue, result_queue, msg_queue):
        """
        Worker which performs tasks from the task queue, post the results to
        the results queue and send status and error messages on the msg queue
        """
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.msg_queue = msg_queue

    def sign(self, next_task):
        """Sign all messages as coming from this worker performing this task"""
        class put_wrapper(object):
            def __init__(self, orig_put, worker, task):
                self.orig_put = orig_put
                self.worker = worker
                self.task = task
            def __call__(self, msg, block=True, timeout=None):
                assert isinstance(msg, PrMsg)
                msg.worker = self.worker.name
                msg.task = self.task
                #msg.log()
                return self.orig_put(msg, block, timeout)

        # Wrap the message queue's put function
        self.msg_queue.put = put_wrapper(self.msg_queue.put, self, next_task)
        self.result_queue.put = put_wrapper(self.result_queue.put, self,
                                            next_task)

    def unsign(self):
        # Unwrap the message queue's put function
        self.msg_queue.put = self.msg_queue.put.orig_put
        self.result_queue.put = self.result_queue.put.orig_put

    def run(self):
        while True:
            # Perform a task off of the queue
            next_task = self.task_queue.get()
            self.sign(next_task)
            if next_task is None:
                # Poison pill means we should exit
                self.msg_queue.put(PrMsg('Taking Poison Pill'))
                break

            self.msg_queue.put(PrMsg('', MsgTypes.STARTED))
            try:
                answer = next_task(self.msg_queue)
            except:
                # The main task didn't execute.  Report an error
                s = StringIO()
                print_exception(sys.exc_info()[0], sys.exc_info()[1],
                                sys.exc_info()[2], None, s)
                answer = "ERROR: " + s.getvalue()
                self.msg_queue.put(PrMsg(answer, MsgTypes.USER_ERROR))
            else:
                self.result_queue.put(PrMsg(answer, MsgTypes.RESULT))
            self.msg_queue.put(PrMsg('', MsgTypes.FINISHED))
            self.unsign()
        return

class Task(object):
    def __init__(self, code, env, key):
        self.code = code
        self.env = env
        self.key = key
        self.id = -1        # Defined by task list

    def __str__(self):
        code = self.code
        if len(code) > 10:
            code = code[:8] + "..."

        return "eval(%s) for cell %s" % (code, self.key)

    __repr__ = __str__

    def __call__(self, msg_queue):
        """Evaluate the user's code"""
        #time.sleep(1)
        answer = eval(self.code, self.env, {})
        return answer

class TaskStatus(object):
    QUEUED, IN_PROCESSING, COMPLETED = range(3)
    CANCELED = 10
class TaskMetadata(object):
    def __init__(self, id, status):
        """Holds metadata related to task accesable by the EvalManager"""
        # All of these defined by the task list
        self.id = id
        self.status = status


class TaskList(list):
    def __init__(self):
        self._meta_tasks = []

    def append(self, obj):
        self.add_task(obj)

    def add_task(self, task):
        id = len(self)
        task.id = id
        meta_task = TaskMetadata(id, TaskStatus.QUEUED)
        self._meta_tasks.append(meta_task)
        list.append(self, task)

    def mark(self, task, status):
        # Accept either the task id or the task itself
        if isinstance(task, Task):
            task_id = task.id
        elif task is int:
            task_id = task
        else:
            raise TypeError
        self._meta_tasks[task_id].status = status

    def cancel_outstanding(self):
        for t in self.jobs_in_processing:
            self.mark(t, TaskStatus.CANCELED)
        for t in self.jobs_in_queue:
            self.mark(t, TaskStatus.CANCELED)

    @property
    def num_jobs(self):
        return len(self.jobs_in_queue)

    @property
    def jobs_in_processing(self):
        return [self[t.id] for t in self._meta_tasks
                if t.status == TaskStatus.IN_PROCESSING]

    @property
    def jobs_in_queue(self):
        return [self[t.id] for t in self._meta_tasks
                    if t.status == TaskStatus.QUEUED]

class FakeQueuePipe(object):
    def recv(self):
        while True: pass
    def poll(self):
        pass
    def put(self,*args):
        pass
    def get_nowait(self):
        raise QueueEmpty

class QueuePipe(object):
    def __init__(self):
        self.recv, self.send = multiprocessing.Pipe(duplex=False)

    def get(self):
        return self.recv.recv()

    def get_nowait(self):
        if self.recv.poll():
            return self.recv.recv()
        else:
            raise QueueEmpty

    def put(self, obj, block=None, timeout=None):
        self.send.send(obj)




class EvalManager(object):
    def __init__(self, main_window, OnResult, OnError):
        """Manages workers and queues for user created code being executed
        or evaluated in spreadsheet cells and macros.

        Parameter OnResult is the function to call back in the UI when a result
        has been found.  It should take ********* """
        self.main_window = main_window
        self.OnResult = OnResult
        self.OnError = OnError


        self.task_queue = QueuePipe()
        self.result_queue = QueuePipe()
        self.message_queue = FakeQueuePipe()

        self.num_workers = 1
        self.last_idle = time.time()
        self.progress = None     # May store a progress dialog if needed
        self.next_task_id = 1
        self.tasks = TaskList()

        # Flags
        self.keep_going = True

        self.start_workers()

    @property
    def num_jobs(self):
        return self.tasks.num_jobs

    def start_workers(self):
        # Start up the workers
        self.workers = []
        for i in xrange(self.num_workers):
            w = Worker(self.task_queue, self.result_queue,
                               self.message_queue)
            w.start()
            self.workers.append(w)

    def add_task(self, code, env, key):
        """Add some code to eval to the queue"""
        task = Task(code, env, key)
        self.tasks.append(task)
        self.task_queue.put(task)
        return task.id

    def terminate(self, no_restart=False):
        """Stop all processes"""

        busy = wx.BusyInfo("Waiting for processes to terminate...")
        self.keep_going = False

        # Stop the workers
        for w in self.workers:
            w.terminate()

        # Wait for them to die
        alive = 1
        while alive:
            alive = sum([w.is_alive() for w in self.workers])
            time.sleep(0.1)

        # Empty the queues
        for q in (self.message_queue, self.task_queue, self.result_queue):
            while True:
                try:
                    trash = q.get_nowait()
                except QueueEmpty:
                    break

        # Tell the task list we killed all outstanding tasks
        self.tasks.cancel_outstanding()

        # Start the workers back up again (for next set of evals)
        if not no_restart:
            self.start_workers()
            self.process_tasks()
        else:
            self.workers = []

        busy.Destroy()

    def process_tasks(self):
        """Once evoked, runs continuously to read result and message queues"""
        self.keep_going = True
        while self.keep_going:
            # Pull off results queue
            try:
                res = self.result_queue.get_nowait()
            except QueueEmpty:
                pass
            else:
                assert res.type == MsgTypes.RESULT
                self.OnResult(res)

            # Pull off message queue
            try:
                msg = self.message_queue.get_nowait()
            except QueueEmpty:
                pass
            else:
                if msg.type == MsgTypes.STARTED:
                    self.tasks.mark(msg.task, TaskStatus.IN_PROCESSING)
                elif msg.type == MsgTypes.FINISHED:
                    self.tasks.mark(msg.task, TaskStatus.COMPLETED)
                self.OnError(msg)

            # Update progress dialog
            self._process_tasks_progress_dialog()

            wx.YieldIfNeeded()

    def _process_tasks_progress_dialog(self):
        """Helper function to manage the progress dialog"""
        progress = self.progress
        if self.num_jobs == 0:
            self.last_idle = time.time()
            if progress is not None:
                progress.Destroy()
                progress = None
        if self.last_idle + 3 < time.time():
            if progress is None:
                # Create the progress dialog
                title = "Working"
                msg = "*"*80
                style = wx.PD_CAN_ABORT | wx.PD_APP_MODAL | wx.PD_SMOOTH

                progress = wx.ProgressDialog(title, msg, self.num_jobs,
                                             self.main_window, style)
                progress.inital_num_jobs = self.num_jobs
            else:
                # Update progress dialog
                msg = "Currently working on: %s \nJobs left: %d"
                msg = msg % (self.tasks.jobs_in_processing, self.num_jobs)
                keep_going, skip = progress.Pulse(msg)
                if not keep_going:
                    self.terminate()
                    progress.Destroy()
                    progress = None
        self.progress = progress

    def __del__(self):
        self.terminate()



################################################################################
# Test Program (with WX)
###############################################################################
class MyFrame(wx.Frame):
    """
    A simple Frame class.
    """
    def __init__(self, parent, id, title):
        """
        Initialise the Frame.
        """

        self.process_manager = EvalManager(self, self.UpdateStatus,
                                           self.UpdateStatus)

        wx.Frame.__init__(self, parent, id, title, wx.Point(700, 500),
                          wx.Size(600, 300))

        # Create the panel, sizer and controls
        self.panel = wx.Panel(self, wx.ID_ANY)
        self.sizer = wx.GridBagSizer(5, 5)

        self.start_bt = wx.Button(self.panel, wx.ID_ANY, "Start")
        self.Bind(wx.EVT_BUTTON, self.OnStart, self.start_bt)
        self.start_bt.SetDefault()
        self.start_bt.SetToolTipString('Start the execution of tasks')
        self.start_bt.ToolTip.Enable(True)

        self.stop_bt = wx.Button(self.panel, wx.ID_ANY, "Stop")
        self.stop_bt.Enable(False)
        self.Bind(wx.EVT_BUTTON, self.OnTerm, self.stop_bt)

        self.output_tc = wx.TextCtrl(self.panel, wx.ID_ANY,
                                     style=wx.TE_MULTILINE|wx.TE_READONLY)

        # Add the controls to the sizer
        self.sizer.Add(self.start_bt, (0, 0),
                       flag=wx.ALIGN_CENTER|wx.LEFT|wx.TOP|wx.RIGHT, border=5)
        self.sizer.Add(self.stop_bt, (0, 1),
                       flag=wx.ALIGN_CENTER|wx.LEFT|wx.TOP|wx.RIGHT, border=5)
        self.sizer.Add(self.output_tc, (1, 0),
                       flag=wx.EXPAND|wx.LEFT|wx.RIGHT|wx.BOTTOM, border=5)
        self.sizer.AddGrowableCol(0)
        self.sizer.AddGrowableRow(1)

        self.panel.SetSizer(self.sizer)

        self.Bind(wx.EVT_CLOSE, self.OnClose)


    def OnStart(self, event):
        """
        Start the execution of tasks by the processes.
        """
        #self.start_bt.Enable(False)
        #self.stop_bt.Enable(True)
        self.output_tc.AppendText('Unordered results...\n')
        # Start processing tasks
        for i in xrange(100):
            self.process_manager.add_task("(10+%d)" % i)
        self.process_manager.process_tasks()

    def OnTerm(self, event):
        self.process_manager.terminate()
        #self.start_bt.Enable(True)
        #self.stop_bt.Enable(False)

    def OnClose(self, event):
        """
        Stop the task queue, terminate processes and close the window.
        """
        self.start_bt.Enable(False)
        self.process_manager.terminate(no_restart=True)
        self.Destroy()

    def UpdateStatus(self, msg):
        self.output_tc.AppendText("Output: %s\n" % msg)


class MyApp(wx.App):
    """
    A simple App class, modified to hold the processes and task queues.
    """
    def __init__(self, redirect=True, filename=None, useBestVisual=False,
                 clearSigInt=True):
        """
        Initialise the App.
        """
        wx.App.__init__(self, redirect, filename, useBestVisual, clearSigInt)

    def OnInit(self):
        """
        Initialise the App with a Frame.
        """
        self.frame = MyFrame(None, -1, 'wxSimpler_MP')
        self.frame.Show(True)
        return True


if __name__ == '__main__':
    # Create the app, including worker processes
    app = MyApp()
    app.MainLoop()