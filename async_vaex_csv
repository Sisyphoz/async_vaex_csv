import vaex as vx
import asyncio
from pathlib import Path
from datetime import datetime
import numpy as np

now = datetime.now()
storage = Path(r'raw\\').glob('*.csv')
time = datetime.now().strftime("%H%M%S")

async def ps():

    for file in storage:

        df = vx.open(file, sep=';')
        print('\n',str(file))

        with vx.progress.tree('rich', title=f"Showcase async execute pipeline for {file.name}:"):

            async with df.executor.auto_execute():

                @vx.delayed
                def process_csv():
                    df = vx.open(file, sep=';')
                    df = df.groupby(by=['Col1', 'Col2']).agg({'Col3': 'sum'})
                    df.rename('Col1_renamed', 'Col2_renamed')
                    time = datetime.now().strftime("%H%M%S")
                    save_dir = str(r'processed\_' + file.name + time + '.csv')
                    df.export_csv(save_dir,progress=None,chunk_size=100000,parallel=True)
                    return df

                res = await process_csv()
                print(res)

loop = asyncio.get_event_loop()
tasks = [loop.create_task(ps())]
wait_task = asyncio.wait(tasks)

if __name__ == '__main__':
    loop.run_until_complete(wait_task)
    # loop.run_forever()
