Basic Usage
======================================

In general, PyStream provides a set of tools to build a data pipeline, especially the one that is targeted for low-latency and high-throughput application.
The data pipeline here can include, for example, an IOT data processing, computer vision at edge, cloud data tabular data analytics, or any other that you can think of. 
These tools will make it easier for you to manage your pipeline, without having to worry about operational stuffs like the data passing and the structure of the pipeline itself.

One important feature that PyStream has provided is the ability to turn your pipeline operation into a parallel operation (through multithreading or multiprocessing).
Depends on the operations you have, you will be able to boost the speed and throughput of your data processing pipeline several times better than when you run it in step-by-step fashion.

A PyStream **pipeline** is made of several **stages** that are linked together.  
The stages are persistent and can be operated autonomously, i.e., it can continuously process data. 
It is also structured in such a way to increase its performance.
You can directly check the `demo notebook <https://github.com/MukhlasAdib/pystream-pipeline/blob/main/demo.ipynb>`_ of PyStream to see how this package is used.
Please visit the `API documentation <https://pystream-pipeline.readthedocs.io/en/latest/api.html>`_ for more detailed information.

To create a pipeline you need to use the following classes:

- Class ``pystream.Pipeline`` is used to construct the pipeline and the interface for you to operate it.
- Class ``pystream.Stage`` is used as the abstract class for your stages, if they are made in form of Python classes.

To make the pipeline, in general you need to do the followings:

1. Create the stages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Stages are basically a bundle of data processing operations that is packed together. 
With PyStream, a stage can be in form of a class instance or a function

If you made it as a class instance you need to make the class inherit from ``pystream.Stage`` abstract class.
For now, the methods that need to be defined are ``__call__`` and ``cleanup``.
See the API documentation to check what methods and interface need to be defined when inherit from it.
The advantage of using ``pystream.Stage`` is that the ``cleanup`` method will be invoked during pipeline cleanup.

If you want to make it as a function or class that does not inherit ``pystream.Stage``, then you only need to make a callable that only takes one argument, which is the data to be processed.
The function also has to return one value, which is the resulted data.

For example, we have a dummy data processing stage that only waits for 0.1 second and increment the integer input data by 1.
In the class form, it will be something like this::

    class DummyStage(pystream.Stage):
        def __init__(self, name: str) -> None:
            self.name = name
            self.wait = 0.1

        def __call__(self, data: int) -> int:
            time.sleep(self.wait)
            print(data)
            return data + 1

        def cleanup(self) -> None:
            print("Stage is clean!")
 
Note that it will print out "Stage is clean!" when you invoke ``cleanup`` method of your pipeline.
In functional form, you can just use ``DummyStage()`` as your callable function, or you can make it a function like::

    def dummy_stage(data: int) -> int:
        time.sleep(1)
        print(data)
        return data + 1

2. Build the pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

After you have defined your stages, then you only need to make a pipeline from them by using ``pystream.Pipeline`` class.
Please check the API documentation to see the available interface.

Creating a pipeline instance is easy. You can do it by instantiate the class with no argument.
Or, you can pass ``input_generator`` argument to define how the pipeline input will be generated if you want the pipeline to be operated autonomously.
Here is a sample of a pipeline creation where you want the input to be ``0`` integer for each pipeline cycle::

    pipeline = pystream.Pipeline(input_generator=lambda: 0)

Then, let's add some stages by using ``add`` method with an optional ``name`` argument to set the stage name::

    pipeline.add(DummyStage(), name="Stage_1") # stage 1
    pipeline.add(DummyStage(), name="Stage_2") # stage 2
    pipeline.add(dummy_stage, name="Stage_3") # stage 3

3. Choose operation mode
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To turn the pipeline into serial mode, you only need to invoke ``serialize`` method.
In serial mode, the stage input data will be passed to the stage 1, and then to stage 2, and lastly to stage 3.
The next data will be passed to stage 1 after stage 3 has been finished.
Thus, in this mode, there is only one data that can be processed and one stage that will be executed at a time::

    pipeline.serialize()

To turn the pipeline into parallel mode, you need to invoke ``parallelize`` method.
In parallel mode, each stage live in a separate thread/process.
The input data will be passed to stage 1 first and then to stage 2, and lastly to stage 3, just like the serial mode.
But in parallel mode, when a stage is done processing, that stage can accept another data immediately, without having to wait the final stage finishing its job.
Therefore, multiple data can be processed and multiple stages can be executed at the same time::

    pipeline.parallelize()

4. Run the pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To operate the pipeline in autonomous mode, you only need to call the ``pipeline.start_loop`` method and pass the ``period`` argument, which is the time interval between each pipeline cycle.
The pipeline will generate and pass the data generated by ``input_generator`` for each ``period`` seconds::

    pipeline.start_loop(period=0.1)

When you start it, you will see that it will print out a lot of ``0``, ``1``, ``2`` which come from the ``print`` statement in the stages.
If you are in serial mode, you will see that the numbers are printed in the right order.
However, in parallel mode the numbers will be printed in random order since all stages keep processing the data at the same time.

You can also do single time execution of the pipeline by calling ``pipeline.forward`` method, which take the input data as the argument (input generator is ignored)::

    pipeline.forward(0)

Note that the method will not give you any result and it is blocking when you are in serial mode. 
To get the latest result, call the ``pipeline.get_results`` method::

    print(pipeline.get_results())

In that case, number ``3`` will be printed if the pipeline has done processing your data.
If it has not been finished, you will get ``None`` instead (for parallel mode).

5. Cleanup the pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you want to shutdown the pipeline, then just call ``pipeline.cleanup()``. It will invoke the ``cleanup`` method of all the stages.
If the pipeline is in autonomous operation mode, you need to stop the input generator by calling ``pipeline.stop_loop()``.

On the other hand, we provide a built-in pipeline profiler that can measure your pipeline's latency and throughput.
The profiler can be activated by specifying ``use_profiler`` to True when instantiating ``pystream.Pipeline``.
To get the pipeline profiles, use ``get_profiles`` method of ``pystream.Pipeline``.
For examples, please check ``demo.ipynb``.
