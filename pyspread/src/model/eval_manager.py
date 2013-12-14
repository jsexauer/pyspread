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
    def __init__(self, task_queue, output_queue):
        """
        Worker which performs tasks from the task queue, post the results to
        the results queue and send status and error messages on the msg queue
        """
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.output_queue = output_queue
        self.log = []

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
        self.output_queue.put = put_wrapper(self.output_queue.put, self,
                                            next_task)

    def unsign(self):
        # Unwrap the message queue's put function
        self.output_queue.put = self.output_queue.put.orig_put

    def run(self):
        while True:
            # Perform a task off of the queue
            self.log.append("%s\t Waiting for a task..." % time.time())
            next_task = self.task_queue.get()
            self.log.append("%s\t Got task %s" % (time.time(), next_task))
            self.sign(next_task)
            if next_task is None:
                # Poison pill means we should exit
                self.output_queue.put(PrMsg('Taking Poison Pill'))
                break

            self.output_queue.put(PrMsg('', MsgTypes.STARTED))
            self.log.append("%s\t Started task %s" % (time.time(), next_task))
            try:
                answer = next_task(self.output_queue)
            except:
                # The main task didn't execute.  Report an error
                s = StringIO()
                print_exception(sys.exc_info()[0], sys.exc_info()[1],
                                sys.exc_info()[2], None, s)
                answer = "ERROR: " + s.getvalue()
                answer = PrMsg(answer, MsgTypes.USER_ERROR)
            else:
                self.log.append("%s\t Put result for task %s on result queue" %
                                (time.time(), next_task))
                answer = PrMsg(answer, MsgTypes.RESULT)
            self.output_queue.put(PrMsg('', MsgTypes.FINISHED))
            self.output_queue.put(answer)
            self.unsign()
            self.log.append("%s\t WORKER Writing log" % time.time())
            f = open(r"C:\log.txt", 'a')
            f.write('\n'+'\n'.join(self.log)+'\n')
            f.close()
            self.log = []
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

        return "<eval(%s) for cell %s>" % (code, self.key)

    __repr__ = __str__

    def __call__(self, output_queue):
        """Evaluate the user's code"""
        #time.sleep(1)
        answer = eval(self.code, self.env, {})
        return answer

class TaskStatus(object):
    QUEUED, IN_PROCESSING, EVALUATED, DISPLAYED = range(4)
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
        # Never allow us to jump more than one status for all "normal
        #   marks" (ie, not canceles)
        cur_status = self._meta_tasks[task_id].status
        if status < TaskStatus.CANCELED:
            assert status - cur_status == 1
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
    def num_not_finished(self):
        return len(self.jobs_not_finished)

    @property
    def jobs_in_processing(self):
        return [self[t.id] for t in self._meta_tasks
                if t.status == TaskStatus.IN_PROCESSING]

    @property
    def jobs_in_queue(self):
        return [self[t.id] for t in self._meta_tasks
                    if t.status == TaskStatus.QUEUED]

    @property
    def jobs_not_finished(self):
        return [self[t.id] for t in self._meta_tasks
                    if t.status < TaskStatus.DISPLAYED]

    def __str__(self):
        return '[' + ',\n '.join([str(self[a.id])+'@'+str(a.status)
                         for a in self._meta_tasks]) + ']'

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
            return self.get()
        else:
            raise QueueEmpty

    def put(self, obj, block=None, timeout=None):
        self.send.send(obj)

class BufferedQueuePipe(QueuePipe):
    def __init__(self):
        super(BufferedQueuePipe, self).__init__()
        self.put_buffer = []
        self.get_buffer = []
    def put(self, obj, block=None, timeout=None):
        self.put_buffer.append(obj)
        if len(self.put_buffer) > 10:
            super(BufferedQueuePipe, self).put(self.put_buffer)
            self.put_buffer = []

    def get_nowait(self):
        if len(self.get_buffer) > 0:
            return self.get_buffer.pop()
        else:
            self.get_buffer = super(BufferedQueuePipe, self).get_nowait()
            return self.get_buffer.pop()
    '''
    def get(self):
        while True:
            try:
                a = self.get_nowait()
                return a
            except QueueEmpty:
                pass
    '''




class EvalManager(object):
    def __init__(self, main_window, OnResult, OnError, OnFinishBatch):
        """Manages workers and queues for user created code being executed
        or evaluated in spreadsheet cells and macros.

        Parameter OnResult is the function to call back in the UI when a result
        has been found.  It should take ********* """
        self.main_window = main_window
        self.OnResult = OnResult
        self.OnError = OnError
        self.OnFinishBatch = OnFinishBatch


        self.task_queue = multiprocessing.Queue()
        self.output_queue = multiprocessing.Queue()

        self.num_workers = 1
        self.last_idle = time.time()
        self.progress = None     # May store a progress dialog if needed
        self.next_task_id = 1
        self.tasks = TaskList()

        # Flags
        self.keep_going = True
        self.in_batch = False

        self.log = []

        self.workers = []
        self.start_workers()

    @property
    def num_jobs(self):
        return self.tasks.num_jobs

    def start_workers(self):
        # Start up the workers
        self.workers = []
        for i in xrange(self.num_workers):
            w = Worker(self.task_queue, self.output_queue)
            w.start()
            self.workers.append(w)

    def add_task(self, code, env, key):
        """Add some code to eval to the queue"""
        task = Task(code, env, key)
        self.tasks.append(task)
        self.task_queue.put(task)
        self.in_batch = True
        self.log.append("%s\t Placed task %s on queue" % (time.time(), task))
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
        for q in (self.task_queue, self.output_queue):
            while True:
                try:
                    trash = q.get_nowait()
                except QueueEmpty:
                    break

        # Tell the task list we killed all outstanding tasks
        self.tasks.cancel_outstanding()

        # Start the workers back up again (for next set of evals)
        if no_restart:
            self.log.append("%s\t EVAL_MANAGER Writing log" % time.time())
            f = open(r"C:\log.txt", 'a')
            f.write('\n'+'\n'.join(self.log)+'\n')
            f.close()
            self.workers = []
        else:
            self.start_workers()
            self.keep_going = True

        busy.Destroy()

    def process_queues(self, event):
        """Evoke during idle event of main window to read output queues"""
        self.log.append("%s\t {{Top of Loop" % time.time())
        while self.keep_going:
            try:
                msg = self.output_queue.get_nowait()
            except QueueEmpty:
                break   # Break out of loop and yield back to wx
            else:
                if msg.type == MsgTypes.STARTED:
                    # Mark task started
                    self.tasks.mark(msg.task, TaskStatus.IN_PROCESSING)
                elif msg.type == MsgTypes.FINISHED:
                    # Mark task finished
                    self.tasks.mark(msg.task, TaskStatus.EVALUATED)
                elif msg.type == MsgTypes.RESULT:
                    # Display result
                    self.log.append("%s\t Pulled result for %s off queue" %
                                    (time.time(), msg.task))
                    self.OnResult(msg, doRefresh=self.num_jobs>0)
                    self.tasks.mark(msg.task, TaskStatus.DISPLAYED)
                else:
                    self.OnError(msg)
                    self.tasks.mark(msg.task, TaskStatus.DISPLAYED)
                # See if we've finished this batch of tasks
                if self.tasks.num_not_finished == 0:
                    self.EndBatch()
                # Update progress dialog
                self._update_progress_dialog()

        self.log.append("%s\t }}Yielding" % time.time())
        event.Skip()

    def _update_progress_dialog(self):
        """Helper function to manage the progress dialog"""
        progress = self.progress
        if not self.in_batch:
            self.last_idle = time.time()
            if progress is not None:
                progress.Destroy()
                progress = None
        elif self.last_idle + 3 < time.time():
            # Update progress dialog
            msg = ("Currently evaluating: %s \n"
                   "Jobs left to be evaluated: %d; "
                   "Jobs left to be displayed: %d")
            msg = msg % (self.tasks.jobs_in_processing, self.num_jobs,
                         self.tasks.num_not_finished)
            if progress is None:
                # Create the progress dialog
                title = "Working"
                style = wx.PD_CAN_ABORT | wx.PD_APP_MODAL | wx.PD_SMOOTH

                progress = wx.ProgressDialog(title, msg, 1,
                                             self.main_window, style)
            keep_going, skip = progress.Pulse(msg)
            if not keep_going:
                self.terminate()
                progress.Destroy()
                progress = None
        self.progress = progress
        self.log.append("%s\t " % time.time() +
                        str(self.tasks).replace('\n',"\n%s\t " % time.time()))
        self.log.append("%s\t NotFin: %s" % (time.time(),
                                          self.tasks.jobs_not_finished))

    def EndBatch(self):
        assert self.task_queue.qsize() == 0
        assert self.output_queue.qsize() == 0
        assert self.tasks.num_not_finished == 0
        self.tasks = TaskList() # Empty the task list
        self.OnFinishBatch()    # Call for grid referesh
        self.in_batch = False

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
        self.process_manager.process_queues()

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