export const search = {
  host: process.env.NEXT_PUBLIC_SEARCH_URL,
  headers: { 
    "Content-Type": "application/json",
    Authorization: `Bearer ${process.env.NEXT_PUBLIC_SEARCH_TOKEN}` 
  },
  options: {},
};