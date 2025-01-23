"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.debug = debug;
function debug(name) {
    return (...args) => {
        const DEBUG = process.env.DEBUG;
        if (DEBUG && name.startsWith(DEBUG.replace("*", "")))
            console.log(name, args);
    };
}
