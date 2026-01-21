This project uses vanillia javascript for it's frontend. It should be converted to typescript.
THe result should be that each page has it's own javascript file that is compiled from all the needed typescript files,
so the code should be allowed, and recommended, to be put in different file and then compiled down to minified javascript files (with source maps), one file for each view (like overview, queues, queue etc.)

First, make sure that the tsconfig.json is correct according to the prompt above
Second, migrate the code from javascript to typescript by adding types. If the code can be rewritten without changing outcome in order to make it more like typescript, do that, before changing, add specs if possible.
Third, compile the typescript source into javascript, one for each view.

When all code has been migrated and you can compile the typescript code using `npx tsc` to javascript it's done.



