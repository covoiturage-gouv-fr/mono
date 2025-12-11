import { MDXRemote } from "next-mdx-remote/rsc";
import rehypeSlug from "rehype-slug";
import remarkGfm from "remark-gfm";
import { VFileCompatible } from "vfile";

export default function MDContent(props: { source: VFileCompatible }) {
  const options = {
    mdxOptions: {
      remarkPlugins: [remarkGfm],
      rehypePlugins: [rehypeSlug],
    },
  };

  return <MDXRemote source={props.source} options={options} />;
}
