
window.driveTheme=(()=>{
  const key='drive_upload_theme',allowed=['system','light','dark'];
  const media=window.matchMedia('(prefers-color-scheme: dark)');
  let preference='system';
  try{const saved=localStorage.getItem(key);if(allowed.includes(saved))preference=saved}catch{}
  function apply(){document.documentElement.dataset.theme=preference==='system'?(media.matches?'dark':'light'):preference;document.documentElement.dataset.themePreference=preference}
  function setPreference(value){if(!allowed.includes(value))return;preference=value;try{localStorage.setItem(key,value)}catch{}apply()}
  const onSystemChange=()=>{if(preference==='system')apply()};
  if(media.addEventListener)media.addEventListener('change',onSystemChange);else if(media.addListener)media.addListener(onSystemChange);
  apply();
  return{getPreference:()=>preference,setPreference};
})();
