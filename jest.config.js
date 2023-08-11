module.exports = {
  // Automatically clear mock calls and instances between every test
  clearMocks: true,
  preset: "@shelf/jest-postgres",
  maxWorkers: 1,

  moduleFileExtensions: ["ts", "js"],

  testPathIgnorePatterns: [
    "/.yalc/",
    "/data/",
    "/_helpers",
    "src/__tests__/utils.ts",
  ],

  transformIgnorePatterns: ["<rootDir>/node_modules/(?!@assemblyscript/.*)"],

  transform: {
    "^.+\\.(ts|js)$": "ts-jest",
  },
};
