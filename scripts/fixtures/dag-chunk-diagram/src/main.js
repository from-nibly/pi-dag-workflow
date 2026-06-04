export function summarizeTasks(tasks) {
  const done = tasks.filter((task) => task.done).length;
  return `${done}/${tasks.length} done`;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  console.log(summarizeTasks([{ title: "try /dag chunk", done: false }]));
}
