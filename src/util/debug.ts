export function debug(name: string) {
  return (...args: any) => {
    const DEBUG = process.env.DEBUG;
    if (DEBUG && name.startsWith(DEBUG.replace("*", ""))) console.log(name, args);
  };
}
