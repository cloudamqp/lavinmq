"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.benchmark = void 0;
const exec_1 = require("@actions/exec");
exports.benchmark = (program) => __awaiter(void 0, void 0, void 0, function* () {
    let output = "";
    let error = "";
    const options = {
        listeners: {
            stdout: (data) => {
                output += data.toString();
            },
            stderr: (data) => {
                error += data.toString();
            }
        }
    };
    let code = yield (exec_1.exec(program, [], options));
    return {
        code,
        output,
        error
    };
});
