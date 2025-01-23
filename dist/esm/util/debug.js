export function debug(name) {
    return (...args) => {
        const DEBUG = process.env.DEBUG;
        if (DEBUG && name.startsWith(DEBUG.replace("*", "")))
            console.log(name, args);
    };
}
