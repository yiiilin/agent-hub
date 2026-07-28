import { cjk } from '@streamdown/cjk';
import { code } from '@streamdown/code';
import { createMathPlugin } from '@streamdown/math';
import { mermaid } from '@streamdown/mermaid';
import { Streamdown, type Components } from 'streamdown';
import 'katex/dist/katex.min.css';
import 'streamdown/styles.css';

const plugins = {
  cjk,
  code,
  math: createMathPlugin({ singleDollarTextMath: true }),
  mermaid
};
const allowedTags = { mark: [], u: [] };
const components: Components = {
  a: ({ node: _node, ...props }) => <a {...props} target="_blank" rel="noreferrer" />
};

export default function ChatMarkdown({ content, streaming }: { content: string; streaming: boolean }) {
  return <Streamdown
    allowedTags={allowedTags}
    components={components}
    controls={false}
    lineNumbers
    mode={streaming ? 'streaming' : 'static'}
    parseIncompleteMarkdown={streaming}
    plugins={plugins}
  >{content}</Streamdown>;
}
