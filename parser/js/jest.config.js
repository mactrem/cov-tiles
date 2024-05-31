module.exports = {
    transform: {
        "^.+\\.ts": "ts-jest",
    },
    testRegex: "test/unit/(.*|(\\.|/)(test|spec))\\.(js|ts)$",
    moduleFileExtensions: ["ts", "js", "json", "node"],
};
