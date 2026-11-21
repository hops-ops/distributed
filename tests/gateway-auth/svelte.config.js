import adapter from '@sveltejs/adapter-node';
export default { kit: { adapter: adapter(), files: { lib: './.generated/lib' } } };
