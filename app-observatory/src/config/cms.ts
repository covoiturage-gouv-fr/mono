export const cms = {
  host: process.env.NEXT_PUBLIC_CMS_URL,
  next: { revalidate: 60 },
  headers: { 
    "Content-Type": "application/json",
    Authorization: `Bearer ${process.env.NEXT_PUBLIC_CMS_TOKEN}` 
  },
  options: {},
  actusByPage: 10,
  ressourcesByPage: 10
};